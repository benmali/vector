//! Main runtime for the `aws_kinesis_streams` source.
//!
//! Implements shard discovery, balanced/explicit shard assignment, per-shard polling
//! loops, and at-least-once delivery via DynamoDB checkpointing.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_sdk_kinesis::{
    Client as KinesisClient,
    error::DisplayErrorContext,
    types::{Shard, ShardIteratorType},
};
use chrono::{TimeZone, Utc};
use tokio::select;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::codec::Decoder as _;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use vector_lib::{
    codecs::StreamDecodingError,
    config::LogNamespace,
    internal_event::{CountByteSize, EventsReceived, InternalEventHandle as _},
    lookup::{metadata_path, PathPrefix},
    EstimatedJsonEncodedSizeOf,
};

use crate::{
    codecs::Decoder,
    config::log_schema,
    event::{BatchNotifier, BatchStatus, Event},
    shutdown::ShutdownSignal,
    sources::aws_kinesis_streams::{
        AwsKinesisStreamsConfig,
        batcher::SequenceTracker,
        checkpointer::{CheckpointerError, KinesisCheckpointer},
    },
    SourceSender,
};

const KINESIS_MAX_RECORDS: i32 = 10_000;
const BACKOFF_INITIAL_MS: u64 = 300;
const BACKOFF_MAX_MS: u64 = 5_000;
// AWS enforces a hard limit of 5 GetRecords calls per shard per second.
const GET_RECORDS_MIN_INTERVAL_MS: u64 = 200;

/// Parsed stream entry from the config `streams` field.
#[derive(Debug, Clone)]
struct StreamEntry {
    /// The stream identifier as it appears in the config (name or ARN, no shard suffix).
    id: String,
    /// Resolved ARN after `DescribeStream`.
    arn: String,
    /// If non-empty, consume only these specific shard IDs (explicit mode).
    explicit_shards: Vec<String>,
}

pub struct KinesisStreamsSource {
    config: AwsKinesisStreamsConfig,
    kinesis: KinesisClient,
    dynamodb: DynamoDbClient,
    decoder: Decoder,
    acknowledgements: bool,
    log_namespace: LogNamespace,
    client_id: String,
}

impl KinesisStreamsSource {
    pub fn new(
        config: AwsKinesisStreamsConfig,
        kinesis: KinesisClient,
        dynamodb: DynamoDbClient,
        decoder: Decoder,
        acknowledgements: bool,
        log_namespace: LogNamespace,
    ) -> crate::Result<Self> {
        Ok(Self {
            config,
            kinesis,
            dynamodb,
            decoder,
            acknowledgements,
            log_namespace,
            client_id: Uuid::new_v4().to_string(),
        })
    }

    pub async fn run(self, out: SourceSender, shutdown: ShutdownSignal) -> Result<(), ()> {
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        tokio::spawn(async move {
            shutdown.await;
            cancel_clone.cancel();
        });

        let streams = match self.parse_streams() {
            Ok(s) => s,
            Err(e) => {
                error!(message = "Failed to parse stream configuration.", error = %e);
                return Err(());
            }
        };

        let explicit_mode = streams.iter().any(|s| !s.explicit_shards.is_empty());

        let checkpointer = match KinesisCheckpointer::new(
            self.dynamodb.clone(),
            self.client_id.clone(),
            self.config.dynamodb.clone(),
            Duration::from_secs(self.config.lease_period_secs),
        )
        .await
        {
            Ok(c) => Arc::new(c),
            Err(e) => {
                error!(message = "Failed to initialise DynamoDB checkpointer.", error = %e);
                return Err(());
            }
        };

        let mut resolved = Vec::with_capacity(streams.len());
        for mut entry in streams {
            match self.resolve_stream_arn(&entry.id).await {
                Ok(arn) => {
                    entry.arn = arn;
                    resolved.push(entry);
                }
                Err(e) => {
                    error!(message = "Failed to resolve Kinesis stream ARN.", stream = %entry.id, error = %e);
                    return Err(());
                }
            }
        }

        let arc_self = Arc::new(self);

        if explicit_mode {
            arc_self
                .run_explicit(resolved, checkpointer, out, cancel)
                .await;
        } else {
            arc_self
                .run_balanced(resolved, checkpointer, out, cancel)
                .await;
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Stream parsing
    // -------------------------------------------------------------------------

    fn parse_streams(&self) -> crate::Result<Vec<StreamEntry>> {
        let mut balanced: Vec<StreamEntry> = Vec::new();
        let mut explicit_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut seen_balanced = false;
        let mut seen_explicit = false;

        for raw in &self.config.streams {
            for part in raw.split(',') {
                let part = part.trim();
                if part.is_empty() {
                    continue;
                }

                let (id, shard) = parse_stream_id(part)?;

                if shard.is_empty() {
                    if seen_explicit {
                        return Err("Cannot mix balanced and explicit-shard streams".into());
                    }
                    seen_balanced = true;
                    balanced.push(StreamEntry {
                        id,
                        arn: String::new(),
                        explicit_shards: vec![],
                    });
                } else {
                    if seen_balanced {
                        return Err("Cannot mix balanced and explicit-shard streams".into());
                    }
                    seen_explicit = true;
                    explicit_map.entry(id).or_default().push(shard);
                }
            }
        }

        if seen_explicit {
            return Ok(explicit_map
                .into_iter()
                .map(|(id, shards)| StreamEntry {
                    id,
                    arn: String::new(),
                    explicit_shards: shards,
                })
                .collect());
        }

        Ok(balanced)
    }

    // -------------------------------------------------------------------------
    // AWS helpers
    // -------------------------------------------------------------------------

    async fn resolve_stream_arn(&self, id: &str) -> crate::Result<String> {
        if id.starts_with("arn:") {
            return Ok(id.to_string());
        }

        let result = self
            .kinesis
            .describe_stream()
            .stream_name(id)
            .send()
            .await
            .map_err(|e| {
                format!(
                    "Failed to describe Kinesis stream '{id}': {}",
                    DisplayErrorContext(&e)
                )
            })?;

        let arn = result
            .stream_description
            .map(|d| d.stream_arn().to_string())
            .ok_or_else(|| format!("No StreamARN in DescribeStream response for '{id}'"))?;

        Ok(arn)
    }

    async fn collect_shards(&self, arn: &str) -> crate::Result<Vec<Shard>> {
        let mut shards = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            let result = if let Some(token) = next_token.take() {
                self.kinesis.list_shards().next_token(token).send().await
            } else {
                self.kinesis.list_shards().stream_arn(arn).send().await
            }
            .map_err(|e| {
                format!(
                    "Failed to list shards for stream '{arn}': {}",
                    DisplayErrorContext(&e)
                )
            })?;

            if let Some(s) = result.shards {
                shards.extend(s);
            }

            match result.next_token {
                Some(t) => next_token = Some(t),
                None => break,
            }
        }

        Ok(shards)
    }

    async fn get_shard_iterator(
        &self,
        arn: &str,
        shard_id: &str,
        sequence: &str,
        start_from_oldest: bool,
    ) -> crate::Result<String> {
        let (iter_type, has_seq) = if !sequence.is_empty() {
            (ShardIteratorType::AfterSequenceNumber, true)
        } else if start_from_oldest {
            (ShardIteratorType::TrimHorizon, false)
        } else {
            (ShardIteratorType::Latest, false)
        };

        let mut req = self
            .kinesis
            .get_shard_iterator()
            .stream_arn(arn)
            .shard_id(shard_id)
            .shard_iterator_type(iter_type);

        if has_seq {
            req = req.starting_sequence_number(sequence);
        }

        let result = req
            .send()
            .await
            .map_err(|e| format!("GetShardIterator error: {}", DisplayErrorContext(&e)))?;

        match result.shard_iterator {
            Some(iter) if !iter.is_empty() => Ok(iter),
            _ => {
                let fallback = self
                    .kinesis
                    .get_shard_iterator()
                    .stream_arn(arn)
                    .shard_id(shard_id)
                    .shard_iterator_type(ShardIteratorType::TrimHorizon)
                    .send()
                    .await
                    .map_err(|e| {
                        format!(
                            "GetShardIterator fallback error: {}",
                            DisplayErrorContext(&e)
                        )
                    })?;

                fallback
                    .shard_iterator
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "Failed to obtain shard iterator".into())
            }
        }
    }

    // -------------------------------------------------------------------------
    // Balanced mode
    // -------------------------------------------------------------------------

    async fn run_balanced(
        self: Arc<Self>,
        streams: Vec<StreamEntry>,
        checkpointer: Arc<KinesisCheckpointer>,
        out: SourceSender,
        cancel: CancellationToken,
    ) {
        let rebalance_interval = Duration::from_secs(self.config.rebalance_period_secs);
        let lease_period = Duration::from_secs(self.config.lease_period_secs);
        let mut task_set: JoinSet<()> = JoinSet::new();

        'outer: loop {
            for stream in &streams {
                let all_shards = match self.collect_shards(&stream.arn).await {
                    Ok(s) => s,
                    Err(e) => {
                        if cancel.is_cancelled() {
                            break 'outer;
                        }
                        error!(message = "Failed to list shards.", stream = %stream.id, error = %e);
                        continue;
                    }
                };

                let checkpoint_data =
                    match checkpointer.get_checkpoints_and_claims(&stream.id).await {
                        Ok(d) => d,
                        Err(e) => {
                            if cancel.is_cancelled() {
                                break 'outer;
                            }
                            error!(message = "Failed to fetch checkpoints.", stream = %stream.id, error = %e);
                            continue;
                        }
                    };

                // Build unclaimed shard map.
                let mut unclaimed: HashMap<String, String> = HashMap::new();
                for shard in &all_shards {
                    let shard_id = shard.shard_id().to_string();
                    let is_finished = shard
                        .sequence_number_range()
                        .and_then(|r| r.ending_sequence_number())
                        .map(|e| e != "null")
                        .unwrap_or(false);

                    if !is_finished
                        || checkpoint_data.shards_with_checkpoints.contains_key(&shard_id)
                    {
                        unclaimed.insert(shard_id, String::new());
                    }
                }

                for (client_id, claims) in &checkpoint_data.client_claims {
                    for claim in claims {
                        let elapsed = Utc::now()
                            .signed_duration_since(claim.lease_timeout)
                            .to_std()
                            .unwrap_or(Duration::ZERO);

                        if elapsed > lease_period * 2 {
                            unclaimed.insert(claim.shard_id.clone(), client_id.clone());
                        } else {
                            unclaimed.remove(&claim.shard_id);
                        }
                    }
                }

                if !unclaimed.is_empty() {
                    for (shard_id, from_client) in &unclaimed {
                        match checkpointer.claim(&stream.id, shard_id, from_client).await {
                            Ok(seq) => {
                                let src = Arc::clone(&self);
                                let chk = Arc::clone(&checkpointer);
                                let out2 = out.clone();
                                let cancel2 = cancel.clone();
                                let stream2 = stream.clone();
                                let shard_id2 = shard_id.clone();
                                task_set.spawn(async move {
                                    src.run_shard_consumer(
                                        stream2, shard_id2, seq, chk, out2, cancel2,
                                    )
                                    .await;
                                });
                            }
                            Err(CheckpointerError::LeaseNotAcquired) => {}
                            Err(e) => {
                                if cancel.is_cancelled() {
                                    break 'outer;
                                }
                                warn!(message = "Failed to claim shard.", shard = %shard_id, error = %e);
                            }
                        }
                    }
                } else {
                    // Consider stealing a shard from an overloaded client.
                    let self_claims = checkpoint_data
                        .client_claims
                        .get(&self.client_id)
                        .map(|c| c.len())
                        .unwrap_or(0);

                    'steal: for (client_id, claims) in &checkpoint_data.client_claims {
                        if *client_id == self.client_id {
                            continue;
                        }
                        if claims.len() > self_claims + 1 {
                            let idx = simple_rand() % claims.len();
                            let to_steal = &claims[idx];
                            match checkpointer
                                .claim(&stream.id, &to_steal.shard_id, client_id)
                                .await
                            {
                                Ok(seq) => {
                                    let src = Arc::clone(&self);
                                    let chk = Arc::clone(&checkpointer);
                                    let out2 = out.clone();
                                    let cancel2 = cancel.clone();
                                    let stream2 = stream.clone();
                                    let shard_id2 = to_steal.shard_id.clone();
                                    task_set.spawn(async move {
                                        src.run_shard_consumer(
                                            stream2, shard_id2, seq, chk, out2, cancel2,
                                        )
                                        .await;
                                    });
                                    break 'steal;
                                }
                                Err(CheckpointerError::LeaseNotAcquired) => {}
                                Err(e) => {
                                    warn!(message = "Failed to steal shard.", shard = %to_steal.shard_id, error = %e);
                                }
                            }
                            break 'steal;
                        }
                    }
                }

                if cancel.is_cancelled() {
                    break 'outer;
                }
            }

            // Reap completed tasks.
            while task_set.try_join_next().is_some() {}

            if cancel.is_cancelled() {
                break;
            }

            select! {
                _ = sleep(rebalance_interval) => {}
                _ = cancel.cancelled() => { break; }
            }
        }

        while task_set.join_next().await.is_some() {}
    }

    // -------------------------------------------------------------------------
    // Explicit mode
    // -------------------------------------------------------------------------

    async fn run_explicit(
        self: Arc<Self>,
        streams: Vec<StreamEntry>,
        checkpointer: Arc<KinesisCheckpointer>,
        out: SourceSender,
        cancel: CancellationToken,
    ) {
        let mut task_set: JoinSet<()> = JoinSet::new();
        let mut pending: Vec<(StreamEntry, String)> = Vec::new();

        for stream in streams {
            for shard_id in &stream.explicit_shards {
                pending.push((stream.clone(), shard_id.clone()));
            }
        }

        while !pending.is_empty() && !cancel.is_cancelled() {
            let mut still_pending = Vec::new();
            for (stream, shard_id) in pending.drain(..) {
                match checkpointer.claim(&stream.id, &shard_id, "").await {
                    Ok(seq) => {
                        let src = Arc::clone(&self);
                        let chk = Arc::clone(&checkpointer);
                        let out2 = out.clone();
                        let cancel2 = cancel.clone();
                        task_set.spawn(async move {
                            src.run_shard_consumer(stream, shard_id, seq, chk, out2, cancel2)
                                .await;
                        });
                    }
                    Err(e) => {
                        if cancel.is_cancelled() {
                            break;
                        }
                        error!(message = "Failed to start shard consumer, will retry.", shard = %shard_id, error = %e);
                        still_pending.push((stream, shard_id));
                    }
                }
            }
            pending = still_pending;

            if !pending.is_empty() {
                select! {
                    _ = sleep(Duration::from_secs(1)) => {}
                    _ = cancel.cancelled() => { break; }
                }
            }
        }

        while task_set.join_next().await.is_some() {}
    }

    // -------------------------------------------------------------------------
    // Per-shard consumer
    // -------------------------------------------------------------------------

    async fn run_shard_consumer(
        self: Arc<Self>,
        stream: StreamEntry,
        shard_id: String,
        starting_sequence: String,
        checkpointer: Arc<KinesisCheckpointer>,
        mut out: SourceSender,
        cancel: CancellationToken,
    ) {
        debug!(
            message = "Starting shard consumer.",
            stream = %stream.id,
            shard = %shard_id,
            client_id = %self.client_id,
        );

        let commit_period = Duration::from_secs(self.config.commit_period_secs);

        let tracker = Arc::new(SequenceTracker::new(
            self.config.checkpoint_limit,
            starting_sequence.clone(),
        ));

        let events_received = register!(EventsReceived);

        let mut iter = match self
            .get_shard_iterator(
                &stream.arn,
                &shard_id,
                &starting_sequence,
                self.config.start_from_oldest,
            )
            .await
        {
            Ok(it) => it,
            Err(e) => {
                error!(message = "Failed to get shard iterator.", shard = %shard_id, error = %e);
                let _ = checkpointer
                    .checkpoint(&stream.id, &shard_id, &starting_sequence, true)
                    .await;
                return;
            }
        };

        let mut backoff_ms = BACKOFF_INITIAL_MS;
        let mut last_commit = tokio::time::Instant::now();
        let mut last_get_records =
            tokio::time::Instant::now() - Duration::from_millis(GET_RECORDS_MIN_INTERVAL_MS);
        let mut shard_finished = false;
        let mut still_owned = true;

        loop {
            if cancel.is_cancelled() {
                break;
            }

            // Periodic checkpoint.
            if last_commit.elapsed() >= commit_period {
                let seq = tracker.acked_sequence();
                match checkpointer
                    .checkpoint(&stream.id, &shard_id, &seq, false)
                    .await
                {
                    Ok(owned) => {
                        still_owned = owned;
                        if !still_owned {
                            debug!(message = "Shard ownership lost; yielding.", shard = %shard_id);
                            let _ = checkpointer
                                .yield_shard(&stream.id, &shard_id, &seq)
                                .await;
                            break;
                        }
                    }
                    Err(e) => {
                        error!(message = "Failed to checkpoint shard.", shard = %shard_id, error = %e);
                    }
                }
                last_commit = tokio::time::Instant::now();
            }

            // Back-pressure: wait while the in-flight cap is full.
            if !tracker.can_accept(1) {
                select! {
                    _ = sleep(Duration::from_millis(10)) => { continue; }
                    _ = cancel.cancelled() => { break; }
                }
            }

            // Enforce a maximum of 5 GetRecords calls per shard per second.
            let elapsed = last_get_records.elapsed();
            let min_interval = Duration::from_millis(GET_RECORDS_MIN_INTERVAL_MS);
            if elapsed < min_interval {
                select! {
                    _ = sleep(min_interval - elapsed) => {}
                    _ = cancel.cancelled() => { break; }
                }
            }
            last_get_records = tokio::time::Instant::now();

            let get_result = self
                .kinesis
                .get_records()
                .stream_arn(&stream.arn)
                .shard_iterator(&iter)
                .limit(KINESIS_MAX_RECORDS)
                .send()
                .await;

            match get_result {
                Err(e) => {
                    let is_expired = e
                        .as_service_error()
                        .map(|se| se.is_expired_iterator_exception())
                        .unwrap_or(false);

                    if is_expired {
                        warn!(message = "Shard iterator expired, refreshing.", shard = %shard_id);
                        let seq = tracker.acked_sequence();
                        match self
                            .get_shard_iterator(
                                &stream.arn,
                                &shard_id,
                                &seq,
                                self.config.start_from_oldest,
                            )
                            .await
                        {
                            Ok(new_iter) => {
                                iter = new_iter;
                                continue;
                            }
                            Err(re) => {
                                error!(message = "Failed to refresh shard iterator.", error = %re);
                            }
                        }
                    } else if !cancel.is_cancelled() {
                        error!(
                            message = "GetRecords error.",
                            shard = %shard_id,
                            error = %DisplayErrorContext(&e),
                        );
                    }

                    let delay = Duration::from_millis(backoff_ms);
                    backoff_ms = (backoff_ms * 2).min(BACKOFF_MAX_MS);
                    select! {
                        _ = sleep(delay) => {}
                        _ = cancel.cancelled() => { break; }
                    }
                    continue;
                }
                Ok(output) => {
                    match output.next_shard_iterator() {
                        Some(next) if !next.is_empty() => iter = next.to_string(),
                        _ => shard_finished = true,
                    }

                    let records = output.records;
                    if records.is_empty() {
                        let delay = Duration::from_millis(backoff_ms);
                        backoff_ms = (backoff_ms * 2).min(BACKOFF_MAX_MS);
                        select! {
                            _ = sleep(delay) => {}
                            _ = cancel.cancelled() => { break; }
                        }
                        if shard_finished {
                            break;
                        }
                        continue;
                    }

                    backoff_ms = BACKOFF_INITIAL_MS;

                    let record_count = records.len() as i64;

                    // Decode each record into events.
                    let mut all_events: Vec<Event> = Vec::with_capacity(records.len());
                    let mut last_sequence = String::new();

                    for record in &records {
                        let data = record.data().as_ref().to_vec();
                        let timestamp = record.approximate_arrival_timestamp().and_then(|ts| {
                            let secs = ts.secs();
                            let nanos = ts.subsec_nanos();
                            Utc.timestamp_opt(secs, nanos).single()
                        });

                        let schema = log_schema();
                        let partition_key = record.partition_key().to_string();
                        let seq_num = record.sequence_number().to_string();

                        let mut buf = bytes::BytesMut::from(data.as_slice());
                        let mut decoder = self.decoder.clone();

                        loop {
                            match decoder.decode_eof(&mut buf) {
                                Ok(Some((decoded, _))) => {
                                    for mut event in decoded {
                                        if let Event::Log(ref mut log) = event {
                                            match self.log_namespace {
                                                LogNamespace::Vector => {
                                                    if let Some(ts) = timestamp {
                                                        log.try_insert(
                                                            metadata_path!(
                                                                "aws_kinesis_streams",
                                                                "timestamp"
                                                            ),
                                                            ts,
                                                        );
                                                    }
                                                    log.insert(
                                                        metadata_path!("vector", "ingest_timestamp"),
                                                        Utc::now(),
                                                    );
                                                    log.try_insert(
                                                        metadata_path!(
                                                            "aws_kinesis_streams",
                                                            "kinesis_stream"
                                                        ),
                                                        stream.id.clone(),
                                                    );
                                                    log.try_insert(
                                                        metadata_path!(
                                                            "aws_kinesis_streams",
                                                            "kinesis_shard"
                                                        ),
                                                        shard_id.clone(),
                                                    );
                                                    log.try_insert(
                                                        metadata_path!(
                                                            "aws_kinesis_streams",
                                                            "kinesis_partition_key"
                                                        ),
                                                        partition_key.clone(),
                                                    );
                                                    log.try_insert(
                                                        metadata_path!(
                                                            "aws_kinesis_streams",
                                                            "kinesis_sequence_number"
                                                        ),
                                                        seq_num.clone(),
                                                    );
                                                }
                                                LogNamespace::Legacy => {
                                                    if let Some(ts) = timestamp {
                                                        if let Some(timestamp_key) =
                                                            schema.timestamp_key()
                                                        {
                                                            log.try_insert(
                                                                (PathPrefix::Event, timestamp_key),
                                                                ts,
                                                            );
                                                        }
                                                    }
                                                    log.try_insert(
                                                        "kinesis_stream",
                                                        stream.id.clone(),
                                                    );
                                                    log.try_insert(
                                                        "kinesis_shard",
                                                        shard_id.clone(),
                                                    );
                                                    log.try_insert(
                                                        "kinesis_partition_key",
                                                        partition_key.clone(),
                                                    );
                                                    log.try_insert(
                                                        "kinesis_sequence_number",
                                                        seq_num.clone(),
                                                    );
                                                }
                                            }
                                        }

                                        events_received.emit(CountByteSize(
                                            1,
                                            event.estimated_json_encoded_size_of(),
                                        ));
                                        all_events.push(event);
                                    }
                                }
                                Ok(None) => break,
                                Err(e) => {
                                    if !e.can_continue() {
                                        break;
                                    }
                                }
                            }
                        }

                        last_sequence = seq_num;
                    }

                    if all_events.is_empty() {
                        // Records decoded to nothing. Nothing was tracked, so do not
                        // call acknowledge (which would decrement in_flight below zero).
                        // Only advance the acked sequence so the next checkpoint reflects
                        // that these records were consumed.
                        tracker.advance_sequence(last_sequence);
                        if shard_finished {
                            break;
                        }
                        continue;
                    }

                    tracker.track(record_count);

                    // Wire up acknowledgements.
                    let (batch, batch_receiver) =
                        BatchNotifier::maybe_new_with_receiver(self.acknowledgements);

                    let events_with_batch: Vec<Event> = all_events
                        .into_iter()
                        .map(|e| e.with_batch_notifier_option(&batch))
                        .collect();

                    drop(batch);

                    // Spawn ack handler.
                    let tracker2 = Arc::clone(&tracker);
                    let ack_seq = last_sequence.clone();
                    let ack_count = record_count;
                    if let Some(receiver) = batch_receiver {
                        tokio::spawn(async move {
                            let status = receiver.await;
                            if status == BatchStatus::Delivered {
                                tracker2.acknowledge(ack_count, ack_seq);
                            } else {
                                tracker2.release(ack_count);
                            }
                        });
                    } else {
                        // No acknowledgement mode: mark delivered immediately.
                        tracker.acknowledge(record_count, last_sequence.clone());
                    }

                    if out.send_batch(events_with_batch).await.is_err() {
                        debug!(message = "Output channel closed, stopping.", shard = %shard_id);
                        break;
                    }

                    if shard_finished {
                        break;
                    }
                }
            }
        }

        // Final cleanup.
        let final_seq = tracker.acked_sequence();
        if shard_finished && still_owned {
            debug!(message = "Shard fully consumed; deleting checkpoint.", shard = %shard_id);
            let _ = checkpointer.delete(&stream.id, &shard_id).await;
        } else if still_owned {
            let _ = checkpointer
                .checkpoint(&stream.id, &shard_id, &final_seq, true)
                .await;
        }

        debug!(
            message = "Shard consumer finished.",
            stream = %stream.id,
            shard = %shard_id,
        );
    }
}

/// Deterministic pseudo-random index based on nanoseconds, used for shard stealing.
fn simple_rand() -> usize {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as usize)
        .unwrap_or(0)
}

/// Split `"stream-name:shard-id"` into `("stream-name", "shard-id")`.
/// Handles ARNs like `"arn:aws:kinesis:us-east-1:123:stream/my-stream:0"`.
pub(crate) fn parse_stream_id(id: &str) -> crate::Result<(String, String)> {
    let (prefix, tail) = if let Some(slash) = id.rfind('/') {
        (&id[..slash + 1], &id[slash + 1..])
    } else {
        ("", id)
    };

    let parts: Vec<&str> = tail.splitn(3, ':').collect();
    match parts.len() {
        1 => Ok((format!("{prefix}{}", parts[0].trim()), String::new())),
        2 => Ok((
            format!("{prefix}{}", parts[0].trim()),
            parts[1].trim().to_string(),
        )),
        _ => Err(format!(
            "Stream '{}' is invalid: only one shard may be specified per entry.",
            id
        )
        .into()),
    }
}
