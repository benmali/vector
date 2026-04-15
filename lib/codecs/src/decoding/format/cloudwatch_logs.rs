use bytes::Bytes;
use chrono::{TimeZone, Utc};
use derivative::Derivative;
use flate2::read::MultiGzDecoder;
use lookup::{event_path, owned_value_path};
use smallvec::{SmallVec, smallvec};
use std::io::Read;
use vector_config::configurable_component;
use vector_core::{
    config::{DataType, LogNamespace, log_schema},
    event::{Event, LogEvent, Value},
    schema,
};
use vrl::value::{Kind, kind::Collection};

use super::{Deserializer, default_lossy};

const fn default_decompress() -> bool {
    true
}

/// Config used to build a `CloudwatchLogsDeserializer`.
#[configurable_component]
#[derive(Debug, Clone, Default)]
pub struct CloudwatchLogsDeserializerConfig {
    /// CloudWatch Logs-specific decoding options.
    #[serde(default, skip_serializing_if = "vector_core::serde::is_default")]
    pub cloudwatch_logs: CloudwatchLogsDeserializerOptions,
}

impl CloudwatchLogsDeserializerConfig {
    /// Creates a new `CloudwatchLogsDeserializerConfig`.
    pub fn new(options: CloudwatchLogsDeserializerOptions) -> Self {
        Self {
            cloudwatch_logs: options,
        }
    }

    /// Build the `CloudwatchLogsDeserializer` from this configuration.
    pub fn build(&self) -> CloudwatchLogsDeserializer {
        CloudwatchLogsDeserializer {
            decompress: self.cloudwatch_logs.decompress,
            lossy: self.cloudwatch_logs.lossy,
        }
    }

    /// Return the type of event build by this deserializer.
    pub fn output_type(&self) -> DataType {
        DataType::Log
    }

    /// The schema produced by the deserializer.
    pub fn schema_definition(&self, log_namespace: LogNamespace) -> schema::Definition {
        schema::Definition::new_with_default_metadata(
            Kind::object(Collection::empty()),
            [log_namespace],
        )
        .with_event_field(
            &owned_value_path!("message"),
            Kind::bytes(),
            Some("message"),
        )
        .with_event_field(&owned_value_path!("id"), Kind::bytes(), None)
        .optional_field(&owned_value_path!("owner"), Kind::bytes(), None)
        .optional_field(&owned_value_path!("logGroup"), Kind::bytes(), None)
        .optional_field(&owned_value_path!("logStream"), Kind::bytes(), None)
        .optional_field(&owned_value_path!("messageType"), Kind::bytes(), None)
        .optional_field(
            &owned_value_path!("subscriptionFilters"),
            Kind::array(Collection::from_unknown(Kind::bytes())),
            None,
        )
        .optional_field(&owned_value_path!("timestamp"), Kind::timestamp(), Some("timestamp"))
    }
}

/// CloudWatch Logs-specific decoding options.
#[configurable_component]
#[derive(Debug, Clone, PartialEq, Eq, Derivative)]
#[derivative(Default)]
pub struct CloudwatchLogsDeserializerOptions {
    /// Whether to decompress the gzip-compressed CloudWatch Logs payload.
    ///
    /// CloudWatch Logs subscription filter records delivered to Kinesis are
    /// gzip-compressed by default. Set this to `false` if the data has already
    /// been decompressed upstream.
    #[serde(
        default = "default_decompress",
        skip_serializing_if = "vector_core::serde::is_default"
    )]
    #[derivative(Default(value = "default_decompress()"))]
    pub decompress: bool,

    /// Determines whether to replace invalid UTF-8 sequences instead of failing.
    ///
    /// When true, invalid UTF-8 sequences are replaced with the
    /// [`U+FFFD REPLACEMENT CHARACTER`][U+FFFD].
    ///
    /// [U+FFFD]: https://en.wikipedia.org/wiki/Specials_(Unicode_block)#Replacement_character
    #[serde(
        default = "default_lossy",
        skip_serializing_if = "vector_core::serde::is_default"
    )]
    #[derivative(Default(value = "default_lossy()"))]
    pub lossy: bool,
}

/// Deserializer that builds `Event`s from a byte frame containing a CloudWatch Logs
/// subscription filter message.
///
/// Each Kinesis record from a CloudWatch Logs subscription filter contains a
/// gzip-compressed JSON envelope. This deserializer decompresses the payload,
/// parses the JSON envelope, and emits one log event per entry in the
/// `logEvents` array. Shared envelope fields (`owner`, `logGroup`, `logStream`,
/// `subscriptionFilters`, `messageType`) are merged into every emitted event.
#[derive(Debug, Clone, Derivative)]
#[derivative(Default)]
pub struct CloudwatchLogsDeserializer {
    #[derivative(Default(value = "default_decompress()"))]
    decompress: bool,
    #[derivative(Default(value = "default_lossy()"))]
    lossy: bool,
}

impl CloudwatchLogsDeserializer {
    fn decompress_bytes(&self, bytes: &[u8]) -> vector_common::Result<Vec<u8>> {
        let mut gz = MultiGzDecoder::new(bytes);
        let mut decompressed = Vec::new();
        gz.read_to_end(&mut decompressed)
            .map_err(|e| format!("Error decompressing CloudWatch Logs data: {e}"))?;
        Ok(decompressed)
    }
}

impl Deserializer for CloudwatchLogsDeserializer {
    fn parse(
        &self,
        bytes: Bytes,
        log_namespace: LogNamespace,
    ) -> vector_common::Result<SmallVec<[Event; 1]>> {
        if bytes.is_empty() {
            return Ok(smallvec![]);
        }

        let raw: Vec<u8> = if self.decompress {
            self.decompress_bytes(&bytes)?
        } else {
            bytes.to_vec()
        };

        let envelope: serde_json::Value = match self.lossy {
            true => serde_json::from_str(&String::from_utf8_lossy(&raw)),
            false => serde_json::from_slice(&raw),
        }
        .map_err(|e| format!("Error parsing CloudWatch Logs JSON: {e:?}"))?;

        let mut envelope = match envelope {
            serde_json::Value::Object(map) => map,
            _ => return Err("CloudWatch Logs payload must be a JSON object".into()),
        };

        let log_events = match envelope.remove("logEvents") {
            Some(serde_json::Value::Array(arr)) => arr,
            Some(_) => return Err("CloudWatch Logs `logEvents` must be an array".into()),
            None => return Err("CloudWatch Logs payload missing `logEvents` field".into()),
        };

        let mut events: SmallVec<[Event; 1]> = SmallVec::with_capacity(log_events.len());

        for log_entry in log_events {
            let mut log_entry = match log_entry {
                serde_json::Value::Object(map) => map,
                _ => continue,
            };

            // Extract and convert the per-event millisecond epoch timestamp.
            let timestamp = log_entry
                .remove("timestamp")
                .and_then(|v| v.as_i64())
                .and_then(|ms| {
                    Utc.timestamp_opt(ms / 1000, ((ms % 1000) * 1_000_000) as u32)
                        .single()
                });

            let mut log = LogEvent::default();

            // Merge shared envelope metadata fields into every event.
            for (key, value) in &envelope {
                log.insert(event_path!(key.as_str()), Value::from(value.clone()));
            }

            // Merge per-event fields (id, message, plus any extras).
            for (key, value) in log_entry {
                log.insert(event_path!(key.as_str()), Value::from(value));
            }

            // Insert the timestamp according to the active log namespace.
            match log_namespace {
                LogNamespace::Legacy => {
                    if let Some(ts) = timestamp {
                        if let Some(timestamp_key) = log_schema().timestamp_key_target_path() {
                            log.try_insert(timestamp_key, ts);
                        }
                    }
                }
                LogNamespace::Vector => {
                    // In the Vector namespace, the codec does not own the source-level
                    // metadata path. Insert the CloudWatch timestamp as a plain event
                    // field so downstream transforms can reference it. The source
                    // (e.g. aws_kinesis_streams) will add its own ingest timestamp in
                    // the metadata section after decoding.
                    if let Some(ts) = timestamp {
                        log.try_insert(event_path!("timestamp"), ts);
                    }
                }
            }

            events.push(Event::Log(log));
        }

        Ok(events)
    }
}

impl From<&CloudwatchLogsDeserializerConfig> for CloudwatchLogsDeserializer {
    fn from(config: &CloudwatchLogsDeserializerConfig) -> Self {
        config.build()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::Compression;
    use flate2::write::GzEncoder;
    use vector_core::config::log_schema;
    use vrl::value::Value;

    use super::*;

    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn sample_payload() -> serde_json::Value {
        serde_json::json!({
            "owner": "123456789012",
            "logGroup": "/aws/lambda/my-function",
            "logStream": "2024/01/01/[$LATEST]abc123",
            "subscriptionFilters": ["my-filter"],
            "messageType": "DATA_MESSAGE",
            "logEvents": [
                {
                    "id": "event-1",
                    "timestamp": 1704067200000_i64,
                    "message": "first log line"
                },
                {
                    "id": "event-2",
                    "timestamp": 1704067201000_i64,
                    "message": "second log line"
                }
            ]
        })
    }

    #[test]
    fn parse_gzip_compressed_payload() {
        let payload = serde_json::to_vec(&sample_payload()).unwrap();
        let compressed = gzip(&payload);
        let deserializer = CloudwatchLogsDeserializer::default();

        for namespace in [LogNamespace::Legacy, LogNamespace::Vector] {
            let events = deserializer
                .parse(Bytes::from(compressed.clone()), namespace)
                .unwrap();
            assert_eq!(events.len(), 2, "expected 2 events for namespace {namespace:?}");
        }
    }

    #[test]
    fn parse_uncompressed_payload() {
        let payload = serde_json::to_vec(&sample_payload()).unwrap();
        let deserializer = CloudwatchLogsDeserializer {
            decompress: false,
            lossy: true,
        };

        let events = deserializer
            .parse(Bytes::from(payload), LogNamespace::Legacy)
            .unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn events_contain_envelope_fields() {
        let payload = serde_json::to_vec(&sample_payload()).unwrap();
        let compressed = gzip(&payload);
        let deserializer = CloudwatchLogsDeserializer::default();

        let events = deserializer
            .parse(Bytes::from(compressed), LogNamespace::Legacy)
            .unwrap();

        let log = events[0].as_log();
        assert_eq!(log["owner"], Value::Bytes("123456789012".into()));
        assert_eq!(log["logGroup"], Value::Bytes("/aws/lambda/my-function".into()));
        assert_eq!(log["logStream"], Value::Bytes("2024/01/01/[$LATEST]abc123".into()));
        assert_eq!(log["messageType"], Value::Bytes("DATA_MESSAGE".into()));
    }

    #[test]
    fn events_contain_per_event_fields() {
        let payload = serde_json::to_vec(&sample_payload()).unwrap();
        let compressed = gzip(&payload);
        let deserializer = CloudwatchLogsDeserializer::default();

        let events = deserializer
            .parse(Bytes::from(compressed), LogNamespace::Legacy)
            .unwrap();

        let log0 = events[0].as_log();
        assert_eq!(log0["id"], Value::Bytes("event-1".into()));
        assert_eq!(log0["message"], Value::Bytes("first log line".into()));

        let log1 = events[1].as_log();
        assert_eq!(log1["id"], Value::Bytes("event-2".into()));
        assert_eq!(log1["message"], Value::Bytes("second log line".into()));
    }

    #[test]
    fn legacy_namespace_sets_timestamp_key() {
        let payload = serde_json::to_vec(&sample_payload()).unwrap();
        let compressed = gzip(&payload);
        let deserializer = CloudwatchLogsDeserializer::default();

        let events = deserializer
            .parse(Bytes::from(compressed), LogNamespace::Legacy)
            .unwrap();

        let log = events[0].as_log();
        if let Some(timestamp_key) = log_schema().timestamp_key() {
            let ts = log.get((lookup::PathPrefix::Event, timestamp_key));
            assert!(ts.is_some(), "expected timestamp field to be set");
            assert!(matches!(ts.unwrap(), Value::Timestamp(_)));
        }
    }

    #[test]
    fn vector_namespace_inserts_timestamp_as_event_field() {
        let payload = serde_json::to_vec(&sample_payload()).unwrap();
        let compressed = gzip(&payload);
        let deserializer = CloudwatchLogsDeserializer::default();

        let events = deserializer
            .parse(Bytes::from(compressed), LogNamespace::Vector)
            .unwrap();

        let log = events[0].as_log();
        let ts = log.get(event_path!("timestamp"));
        assert!(ts.is_some(), "expected timestamp event field for Vector namespace");
        assert!(matches!(ts.unwrap(), Value::Timestamp(_)));
    }

    #[test]
    fn error_on_missing_log_events_field() {
        let payload = serde_json::json!({ "owner": "123", "messageType": "DATA_MESSAGE" });
        let json_bytes = serde_json::to_vec(&payload).unwrap();
        let deserializer = CloudwatchLogsDeserializer {
            decompress: false,
            lossy: true,
        };

        assert!(
            deserializer
                .parse(Bytes::from(json_bytes), LogNamespace::Legacy)
                .is_err()
        );
    }

    #[test]
    fn empty_input_returns_no_events() {
        let deserializer = CloudwatchLogsDeserializer::default();
        for namespace in [LogNamespace::Legacy, LogNamespace::Vector] {
            let events = deserializer.parse(Bytes::new(), namespace).unwrap();
            assert!(events.is_empty());
        }
    }

    #[test]
    fn empty_log_events_array_returns_no_events() {
        let payload = serde_json::json!({
            "owner": "123456789012",
            "logGroup": "group",
            "logStream": "stream",
            "subscriptionFilters": [],
            "messageType": "DATA_MESSAGE",
            "logEvents": []
        });
        let json_bytes = serde_json::to_vec(&payload).unwrap();
        let compressed = gzip(&json_bytes);
        let deserializer = CloudwatchLogsDeserializer::default();

        let events = deserializer
            .parse(Bytes::from(compressed), LogNamespace::Legacy)
            .unwrap();
        assert!(events.is_empty());
    }
}
