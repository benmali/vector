#![cfg(feature = "aws-kinesis-streams-source-integration-tests")]
#![cfg(test)]

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use aws_sdk_kinesis::{
    Client,
    types::{PutRecordsRequestEntry, ScalingType, Shard, StreamStatus},
};
use aws_smithy_types::Blob;
use futures::StreamExt;
use serial_test::serial;
use tokio::time::sleep;
use vector_lib::{
    codecs::JsonDeserializerConfig,
    lookup::{PathPrefix, path},
};

use crate::{
    SourceSender,
    aws::{AwsAuthentication, RegionOrEndpoint, create_client},
    common::{dynamodb::DynamoDbClientBuilder, kinesis::KinesisClientBuilder},
    config::{ProxyConfig, SourceAcknowledgementsConfig, SourceConfig, SourceContext},
    event::{Event, EventStatus},
    serde::default_framing_message_based,
    sources::aws_kinesis_streams::{
        AwsKinesisStreamsConfig, DynamoDbCheckpointConfig, closed_shard::is_shard_closed,
    },
    test_util::{random_string, trace_init},
};

const PUT_BATCH_SIZE: usize = 500;
/// Enough unread parent records at each split without a multi-minute soak.
/// Override with `KINESIS_RESHARD_RECORDS` for a heavier run.
const DEFAULT_RECORD_COUNT: usize = 2_000;
const INITIAL_SHARD_COUNT: i32 = 2;
/// Uniform scale steps applied while the consumer is live (2 → 4 → 8).
const SHARD_SCALE_TARGETS: [i32; 2] = [4, 8];
const STREAM_READY_TIMEOUT: Duration = Duration::from_secs(120);
const CONSUME_TIMEOUT: Duration = Duration::from_secs(300);
const RESHARD_PROBE_TIMEOUT: Duration = Duration::from_secs(60);
/// Wait after every unique id is seen so a late parent replay is not missed.
const DUPLICATE_SETTLE: Duration = Duration::from_secs(3);
/// AWS `GetRecords` hard cap; also the source `KINESIS_MAX_RECORDS` ceiling.
const GET_RECORDS_MAX: i32 = 10_000;
/// Match source defaults so this is reshard lineage, not 5s lease steal.
const LEASE_PERIOD_SECS: u64 = 30;
const REBALANCE_PERIOD_SECS: u64 = 30;
const TEST_STREAM_PREFIXES: [&str; 2] = ["reshard-", "stable-"];
const CHECKPOINT_TABLE_PREFIX: &str = "vector-kinesis-cp-";
const PUT_RECORDS_TIMEOUT: Duration = Duration::from_secs(30);

fn kinesis_address() -> String {
    std::env::var("KINESIS_ADDRESS").unwrap_or_else(|_| "http://localhost:4566".into())
}

fn record_count() -> usize {
    std::env::var("KINESIS_RESHARD_RECORDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|count| *count >= SHARD_SCALE_TARGETS.len() + 1)
        .unwrap_or(DEFAULT_RECORD_COUNT)
}

fn gen_name(prefix: &str) -> String {
    format!("{prefix}-{}", random_string(10).to_lowercase())
}

async fn dynamodb_client() -> aws_sdk_dynamodb::Client {
    let region = RegionOrEndpoint::with_both("us-east-1", kinesis_address());
    create_client::<DynamoDbClientBuilder>(
        &DynamoDbClientBuilder {},
        &AwsAuthentication::test_auth(),
        region.region(),
        region.endpoint(),
        &ProxyConfig::default(),
        None,
        None,
    )
    .await
    .expect("dynamodb client")
}

async fn client() -> Client {
    let region = RegionOrEndpoint::with_both("us-east-1", kinesis_address());
    create_client::<KinesisClientBuilder>(
        &KinesisClientBuilder {},
        &AwsAuthentication::test_auth(),
        region.region(),
        region.endpoint(),
        &ProxyConfig::default(),
        None,
        None,
    )
    .await
    .expect("kinesis client")
}

async fn wait_until_status(
    client: &Client,
    stream_name: &str,
    wanted: StreamStatus,
    timeout: Duration,
) {
    let started = Instant::now();
    loop {
        let description = client
            .describe_stream()
            .stream_name(stream_name)
            .send()
            .await
            .expect("describe_stream")
            .stream_description
            .expect("stream description");
        if description.stream_status == wanted {
            return;
        }
        if started.elapsed() > timeout {
            panic!(
                "timed out waiting for stream {stream_name} to become {wanted:?}; last status {:?}",
                description.stream_status
            );
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn list_stream_names(client: &Client) -> Vec<String> {
    client
        .list_streams()
        .limit(100)
        .send()
        .await
        .expect("list_streams")
        .stream_names
}

async fn delete_leftover_test_streams(client: &Client) {
    for name in list_stream_names(client).await {
        if TEST_STREAM_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
        {
            let _ = client.delete_stream().stream_name(&name).send().await;
        }
    }
}

async fn delete_leftover_checkpoint_tables(client: &aws_sdk_dynamodb::Client) {
    let tables = client
        .list_tables()
        .send()
        .await
        .expect("list_tables")
        .table_names
        .unwrap_or_default();
    for name in tables {
        if name.starts_with(CHECKPOINT_TABLE_PREFIX) {
            let _ = client.delete_table().table_name(&name).send().await;
        }
    }
}

async fn ensure_stream(client: &Client, stream_name: &str, shard_count: i32) {
    client
        .create_stream()
        .stream_name(stream_name)
        .shard_count(shard_count)
        .send()
        .await
        .unwrap_or_else(|error| panic!("create_stream {stream_name}: {error:?}"));
    wait_until_status(
        client,
        stream_name,
        StreamStatus::Active,
        STREAM_READY_TIMEOUT,
    )
    .await;
}

async fn list_shards(client: &Client, stream_name: &str) -> Vec<Shard> {
    let mut shards = Vec::new();
    let mut next_token: Option<String> = None;
    loop {
        let mut request = client.list_shards().stream_name(stream_name);
        if let Some(token) = next_token {
            request = client.list_shards().next_token(token);
        }
        let response = request.send().await.expect("list_shards");
        shards.extend(response.shards.unwrap_or_default());
        match response.next_token {
            Some(token) if !token.is_empty() => next_token = Some(token),
            _ => break,
        }
    }
    shards
}

fn open_shards(shards: &[Shard]) -> Vec<&Shard> {
    shards
        .iter()
        .filter(|shard| {
            let ending = shard
                .sequence_number_range()
                .and_then(|range| range.ending_sequence_number());
            !is_shard_closed(ending)
        })
        .collect()
}

fn shard_is_closed(shard: &Shard) -> bool {
    let ending = shard
        .sequence_number_range()
        .and_then(|range| range.ending_sequence_number());
    is_shard_closed(ending)
}

fn hash_key_midpoint(starting: &str, ending: &str) -> String {
    let start: u128 = starting
        .parse()
        .unwrap_or_else(|_| panic!("starting hash key {starting}"));
    let end: u128 = ending
        .parse()
        .unwrap_or_else(|_| panic!("ending hash key {ending}"));
    (start + (end - start) / 2).to_string()
}

async fn split_open_shards(client: &Client, stream_name: &str) {
    let shards = list_shards(client, stream_name).await;
    let splits: Vec<(String, String)> = open_shards(&shards)
        .into_iter()
        .map(|parent| {
            let range = parent.hash_key_range().expect("hash key range").clone();
            (
                parent.shard_id().to_string(),
                hash_key_midpoint(range.starting_hash_key(), range.ending_hash_key()),
            )
        })
        .collect();
    assert!(
        !splits.is_empty(),
        "no open shard to split on {stream_name}"
    );
    for (shard_id, mid) in splits {
        tokio::time::timeout(
            RESHARD_PROBE_TIMEOUT,
            client
                .split_shard()
                .stream_name(stream_name)
                .shard_to_split(shard_id)
                .new_starting_hash_key(mid)
                .send(),
        )
        .await
        .expect("SplitShard timed out")
        .unwrap_or_else(|error| panic!("SplitShard failed: {error:?}"));
        wait_until_status(
            client,
            stream_name,
            StreamStatus::Active,
            STREAM_READY_TIMEOUT,
        )
        .await;
    }
}

async fn scale_shard_count(client: &Client, stream_name: &str, target_shard_count: i32) {
    let update_error = match tokio::time::timeout(
        RESHARD_PROBE_TIMEOUT,
        client
            .update_shard_count()
            .stream_name(stream_name)
            .target_shard_count(target_shard_count)
            .scaling_type(ScalingType::UniformScaling)
            .send(),
    )
    .await
    {
        Ok(Ok(_)) => None,
        Ok(Err(error)) => Some(format!("{error:?}")),
        Err(_) => Some("UpdateShardCount timed out".to_string()),
    };

    if let Some(update_error) = update_error {
        eprintln!(
            "UpdateShardCount to {target_shard_count} failed ({update_error}); falling back to SplitShard on every open shard"
        );
        split_open_shards(client, stream_name).await;
    }

    wait_until_status(
        client,
        stream_name,
        StreamStatus::Active,
        STREAM_READY_TIMEOUT,
    )
    .await;
    let shards = list_shards(client, stream_name).await;
    let open = open_shards(&shards);
    assert!(
        open.len() >= target_shard_count as usize,
        "expected at least {target_shard_count} open shards after reshard, found {}",
        open.len()
    );
}

async fn put_id_records(client: &Client, stream_name: &str, start: usize, end: usize) {
    let mut id = start;
    while id < end {
        let batch_end = (id + PUT_BATCH_SIZE).min(end);
        let mut pending: Vec<PutRecordsRequestEntry> = (id..batch_end)
            .map(|record_id| {
                PutRecordsRequestEntry::builder()
                    .data(Blob::new(format!(r#"{{"id":"{record_id}"}}"#)))
                    .partition_key(format!("key-{record_id}"))
                    .build()
                    .expect("put record fields")
            })
            .collect();

        let mut put_attempts = 0u32;
        while !pending.is_empty() {
            put_attempts += 1;
            if put_attempts > 30 {
                panic!(
                    "put_records kept failing after 30 attempts; {} records remaining",
                    pending.len()
                );
            }
            let response = tokio::time::timeout(
                PUT_RECORDS_TIMEOUT,
                client
                    .put_records()
                    .stream_name(stream_name)
                    .set_records(Some(pending.clone()))
                    .send(),
            )
            .await
            .expect("put_records timed out")
            .expect("put_records");
            let mut remaining = Vec::new();
            for (index, result) in response.records().iter().enumerate() {
                if result.error_code().is_some() {
                    remaining.push(pending[index].clone());
                }
            }
            pending = remaining;
            if !pending.is_empty() {
                sleep(Duration::from_millis(200)).await;
            }
        }
        id = batch_end;
    }
}

fn event_id(event: &Event) -> String {
    event
        .as_log()
        .get((PathPrefix::Event, path!("id")))
        .map(|value| value.to_string_lossy().into_owned())
        .expect("decoded record must contain id")
}

fn event_shard(event: &Event) -> String {
    event
        .as_log()
        .get((PathPrefix::Event, path!("kinesis_shard")))
        .map(|value| value.to_string_lossy().into_owned())
        .expect("decoded record must contain kinesis_shard")
}

fn source_config(stream: String, table: String) -> AwsKinesisStreamsConfig {
    AwsKinesisStreamsConfig {
        region: RegionOrEndpoint::with_both("us-east-1", kinesis_address().as_str()),
        auth: AwsAuthentication::test_auth(),
        streams: vec![stream],
        dynamodb: DynamoDbCheckpointConfig {
            table,
            create: true,
            commit_period_secs: 1,
            ..Default::default()
        },
        checkpoint_limit: GET_RECORDS_MAX as u32,
        max_records_per_call: GET_RECORDS_MAX,
        lease_period_secs: LEASE_PERIOD_SECS,
        rebalance_period_secs: REBALANCE_PERIOD_SECS,
        start_from_oldest: true,
        poll_interval_ms: 200,
        framing: default_framing_message_based(),
        decoding: JsonDeserializerConfig::default().into(),
        acknowledgements: SourceAcknowledgementsConfig::from(true),
        ..Default::default()
    }
}

fn spawn_source(config: AwsKinesisStreamsConfig) -> Arc<Mutex<Vec<Event>>> {
    let (tx, mut rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let received: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));
    let received_for_task = Arc::clone(&received);
    tokio::spawn(async move {
        while let Some(event) = rx.next().await {
            received_for_task.lock().expect("received lock").push(event);
        }
    });
    tokio::spawn(async move {
        config
            .build(SourceContext::new_test(tx, None))
            .await
            .expect("build source")
            .await
            .expect("run source");
    });
    received
}

async fn wait_for_event_count(received: &Mutex<Vec<Event>>, min: usize, timeout: Duration) {
    let started = Instant::now();
    loop {
        let count = received.lock().expect("received lock").len();
        if count >= min {
            return;
        }
        if started.elapsed() > timeout {
            panic!("timed out waiting for {min} events; have {count}");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_unique_ids(
    received: &Mutex<Vec<Event>>,
    expected: &HashSet<String>,
    timeout: Duration,
) -> Vec<Event> {
    let started = Instant::now();
    loop {
        let snapshot = received.lock().expect("received lock").clone();
        let unique: HashSet<String> = snapshot.iter().map(event_id).collect();
        if unique.len() >= expected.len() {
            sleep(DUPLICATE_SETTLE).await;
            return received.lock().expect("received lock").clone();
        }
        if started.elapsed() > timeout {
            let missing: Vec<_> = expected.difference(&unique).take(20).cloned().collect();
            panic!(
                "timed out waiting for {} unique ids; have {}, sample missing: {missing:?}",
                expected.len(),
                unique.len()
            );
        }
        sleep(Duration::from_millis(250)).await;
    }
}

fn next_wave_end(produced: usize, wave_size: usize, total: usize, is_last: bool) -> usize {
    if is_last { total } else { produced + wave_size }
}

fn assert_exactly_once(events: &[Event], expected: &HashSet<String>, context: &str) {
    let ids: Vec<String> = events.iter().map(event_id).collect();
    let unique: HashSet<String> = ids.iter().cloned().collect();
    let missing: Vec<_> = expected.difference(&unique).cloned().collect();
    let mut extras: HashMap<String, usize> = HashMap::new();
    for id in &ids {
        *extras.entry(id.clone()).or_insert(0) += 1;
    }
    extras.retain(|_, count| *count > 1);

    let mut shards_for_id: HashMap<String, HashSet<String>> = HashMap::new();
    for event in events {
        shards_for_id
            .entry(event_id(event))
            .or_default()
            .insert(event_shard(event));
    }
    let cross_shard: Vec<_> = shards_for_id
        .iter()
        .filter(|(_, shards)| shards.len() > 1)
        .map(|(id, shards)| (id.clone(), shards.clone()))
        .take(20)
        .collect();

    assert!(
        missing.is_empty(),
        "lost {} records {context}; sample {missing:?}",
        missing.len()
    );
    assert!(
        cross_shard.is_empty(),
        "same record id from multiple shards {context} (parent/child overlap); sample {cross_shard:?}"
    );
    assert!(
        extras.is_empty(),
        "duplicated {} record ids {context} (same-shard replay); sample {:?}",
        extras.len(),
        extras.keys().take(20).collect::<Vec<_>>()
    );
    assert_eq!(ids.len(), unique.len());
    assert_eq!(unique, *expected);
}

fn assert_consumed_closed_and_open_shards(events: &[Event], shards: &[Shard]) {
    let seen: HashSet<String> = events.iter().map(event_shard).collect();
    let closed_seen: Vec<_> = shards
        .iter()
        .filter(|shard| shard_is_closed(shard) && seen.contains(shard.shard_id()))
        .map(|shard| shard.shard_id().to_string())
        .collect();
    let open_seen: Vec<_> = shards
        .iter()
        .filter(|shard| !shard_is_closed(shard) && seen.contains(shard.shard_id()))
        .map(|shard| shard.shard_id().to_string())
        .collect();
    assert!(
        !closed_seen.is_empty(),
        "expected events from closed parent shards after reshard, found {seen:?}"
    );
    assert!(
        open_seen.len() >= SHARD_SCALE_TARGETS[0] as usize,
        "expected events from at least {} open child shards after reshard, found {open_seen:?}",
        SHARD_SCALE_TARGETS[0]
    );
}

async fn put_remaining_waves(
    client: &Client,
    stream: &str,
    mut produced: usize,
    wave_size: usize,
    total: usize,
    reshard: bool,
) {
    for index in 0..SHARD_SCALE_TARGETS.len() {
        if reshard {
            scale_shard_count(client, stream, SHARD_SCALE_TARGETS[index]).await;
        }
        let is_last = index + 1 == SHARD_SCALE_TARGETS.len();
        let next = next_wave_end(produced, wave_size, total, is_last);
        put_id_records(client, stream, produced, next).await;
        produced = next;
    }
}

async fn run_unique_id_delivery(reshard: bool) {
    trace_init();

    let total = record_count();
    let wave_size = total / (SHARD_SCALE_TARGETS.len() + 1);
    let client = client().await;
    let _ = tokio::time::timeout(Duration::from_secs(20), async {
        delete_leftover_test_streams(&client).await;
        delete_leftover_checkpoint_tables(&dynamodb_client().await).await;
    })
    .await;

    let prefix = if reshard { "reshard" } else { "stable" };
    let stream = gen_name(prefix);
    let table = gen_name("vector-kinesis-cp");
    let context = if reshard {
        "after stacked 2→4→8 reshard"
    } else {
        "on stable 2-shard stream (no reshard)"
    };

    ensure_stream(&client, &stream, INITIAL_SHARD_COUNT).await;
    put_id_records(&client, &stream, 0, wave_size).await;
    let received = spawn_source(source_config(stream.clone(), table));
    wait_for_event_count(received.as_ref(), 1, Duration::from_secs(60)).await;
    put_remaining_waves(&client, &stream, wave_size, wave_size, total, reshard).await;

    let expected: HashSet<String> = (0..total).map(|id| id.to_string()).collect();
    let events = wait_for_unique_ids(received.as_ref(), &expected, CONSUME_TIMEOUT).await;
    assert_exactly_once(&events, &expected, context);

    if reshard {
        let shards = list_shards(&client, &stream).await;
        assert_consumed_closed_and_open_shards(&events, &shards);
    }

    let _ = client.delete_stream().stream_name(&stream).send().await;
}

#[tokio::test(flavor = "current_thread", start_paused = false)]
#[serial]
async fn kinesis_stable_shards_no_duplicate_records() {
    run_unique_id_delivery(false).await;
}

#[tokio::test(flavor = "current_thread", start_paused = false)]
#[serial]
async fn kinesis_reshard_no_duplicate_records() {
    run_unique_id_delivery(true).await;
}
