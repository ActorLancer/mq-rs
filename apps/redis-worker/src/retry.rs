use anyhow::Context;
use redis::{aio::MultiplexedConnection, streams::StreamId};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub retry_zset: String,
    pub retry_payloads: String,
    pub dead_stream: String,
    pub dead_index: String,
    /// Number of delayed retries after the initial delivery.
    pub max_retries: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub scheduler_interval: Duration,
    pub scheduler_batch_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    Retryable,
    Permanent,
}

impl FailureClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::Permanent => "permanent",
        }
    }
}

#[derive(Debug)]
pub enum FailureAction {
    RetryScheduled { retry_count: u32, due_at_ms: u64 },
    DeadLettered { dead_stream_id: String },
    AlreadyHandled,
}

#[derive(Debug, Serialize, Deserialize)]
struct RetryEnvelope {
    event_id: String,
    event_type: String,
    payload: String,
    retry_count: u32,
    source_stream_id: String,
    last_error: String,
}

const SCHEDULE_RETRY_SCRIPT: &str = r#"
local source_type = redis.call('TYPE', KEYS[1]).ok
if source_type ~= 'stream' then
    return redis.error_reply('source key must be a stream')
end

local retry_type = redis.call('TYPE', KEYS[2]).ok
if retry_type ~= 'none' and retry_type ~= 'zset' then
    return redis.error_reply('retry schedule key must be a zset')
end

local payload_type = redis.call('TYPE', KEYS[3]).ok
if payload_type ~= 'none' and payload_type ~= 'hash' then
    return redis.error_reply('retry payload key must be a hash')
end

-- Do not schedule a message that another worker has already ACKed.
local pending = redis.call(
    'XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1
)
if #pending == 0 then
    return 0
end

-- The member is deterministic for (logical consumer, event ID, retry number).
-- Retrying this script after an ambiguous network error updates the same item.
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
redis.call('HSET', KEYS[3], ARGV[4], ARGV[5])

return redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
"#;

const DEAD_LETTER_SCRIPT: &str = r#"
local source_type = redis.call('TYPE', KEYS[1]).ok
if source_type ~= 'stream' then
    return redis.error_reply('source key must be a stream')
end

local dead_type = redis.call('TYPE', KEYS[2]).ok
if dead_type ~= 'none' and dead_type ~= 'stream' then
    return redis.error_reply('dead-letter key must be a stream')
end

local index_type = redis.call('TYPE', KEYS[3]).ok
if index_type ~= 'none' and index_type ~= 'hash' then
    return redis.error_reply('dead-letter index key must be a hash')
end

-- Make a repeated transfer idempotent by indexing the original Stream ID.
local previous_id = redis.call('HGET', KEYS[3], ARGV[2])
if previous_id then
    local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[3])
    return {previous_id, acked, 0}
end

local pending = redis.call(
    'XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1
)
if #pending == 0 then
    return {'', 0, 0}
end

local dead_id = redis.call(
    'XADD', KEYS[2], '*',
    'event_id', ARGV[4],
    'event_type', ARGV[5],
    'payload', ARGV[6],
    'source_stream', KEYS[1],
    'source_stream_id', ARGV[3],
    'consumer_group', ARGV[1],
    'logical_consumer_name', ARGV[7],
    'retry_count', ARGV[8],
    'failure_class', ARGV[9],
    'error', ARGV[10],
    'failed_at_ms', ARGV[11]
)

redis.call('HSET', KEYS[3], ARGV[2], dead_id)
local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[3])

return {dead_id, acked, 1}
"#;

const PUBLISH_DUE_RETRY_SCRIPT: &str = r#"
local score = redis.call('ZSCORE', KEYS[2], ARGV[1])
if not score or tonumber(score) > tonumber(ARGV[2]) then
    return false
end

local payload = redis.call('HGET', KEYS[3], ARGV[1])
if not payload then
    return redis.error_reply('due retry has no payload in retry hash')
end

local source_type = redis.call('TYPE', KEYS[1]).ok
if source_type ~= 'none' and source_type ~= 'stream' then
    return redis.error_reply('source key must be a stream')
end

local item = cjson.decode(payload)

-- XADD and removal from the schedule are one atomic Redis operation.
-- If the response is lost, the next scheduler sees no ZSET member and does
-- not publish the same retry a second time.
local stream_id = redis.call(
    'XADD', KEYS[1], '*',
    'event_id', item.event_id,
    'event_type', item.event_type,
    'payload', item.payload,
    'retry_count', tostring(item.retry_count),
    'retry_source_id', item.source_stream_id,
    'last_error', item.last_error
)

redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])

return stream_id
"#;

/// Redis Cluster requires all keys touched by one script to share a hash slot.
/// Requiring the same explicit hash tag makes that invariant clear and testable.
pub fn validate_hash_tags(stream: &str, config: &RetryConfig) -> anyhow::Result<()> {
    let expected = hash_tag(stream)
        .context("STREAM_NAME must contain a non-empty Redis Cluster hash tag, e.g. {events}")?;

    for (name, key) in [
        ("RETRY_ZSET", config.retry_zset.as_str()),
        ("RETRY_PAYLOADS", config.retry_payloads.as_str()),
        ("DLQ_STREAM", config.dead_stream.as_str()),
        ("DLQ_INDEX", config.dead_index.as_str()),
    ] {
        let actual = hash_tag(key).with_context(|| {
            format!("{name} must contain the same Redis hash tag as STREAM_NAME")
        })?;

        if actual != expected {
            anyhow::bail!(
                "{name} hash tag must match STREAM_NAME: expected {{{expected}}}, got {{{actual}}}"
            );
        }
    }

    Ok(())
}

fn hash_tag(key: &str) -> Option<&str> {
    let start = key.find('{')?;
    let end = key[start + 1..].find('}')? + start + 1;
    (end > start + 1).then_some(&key[start + 1..end])
}

pub fn classify_error(error: &anyhow::Error) -> FailureClass {
    use sqlx::Error as SqlxError;

    let Some(sqlx_error) = error.downcast_ref::<SqlxError>() else {
        // Payload/schema/business-state errors are permanent by default.
        return FailureClass::Permanent;
    };

    match sqlx_error {
        SqlxError::PoolTimedOut | SqlxError::PoolClosed | SqlxError::Io(_) => {
            FailureClass::Retryable
        }
        SqlxError::Database(database_error) => {
            let code = database_error.code();
            let code = code.as_deref().unwrap_or_default();

            if code.starts_with("08")
                || matches!(code, "40001" | "40P01" | "53300" | "57P01" | "57P03")
            {
                FailureClass::Retryable
            } else {
                FailureClass::Permanent
            }
        }
        _ => FailureClass::Permanent,
    }
}

fn exponential_delay_ms(attempt: u32, base_ms: u64, max_ms: u64) -> u64 {
    if attempt == 0 || base_ms == 0 || max_ms == 0 {
        return 0;
    }

    let exponent = attempt.saturating_sub(1).min(31);
    base_ms.saturating_mul(1_u64 << exponent).min(max_ms)
}

fn retry_delay(attempt: u32, config: &RetryConfig) -> Duration {
    let base_ms = config.base_delay.as_millis().min(u64::MAX as u128) as u64;
    let max_ms = config.max_delay.as_millis().min(u64::MAX as u128) as u64;
    let ceiling = exponential_delay_ms(attempt, base_ms, max_ms);

    // Jitter spreads retries across the upper half of the backoff window.
    let lower = ceiling / 2;
    let jitter_width = ceiling.saturating_sub(lower).max(1);
    let jitter = (Uuid::new_v4().as_u128() % u128::from(jitter_width)) as u64;

    Duration::from_millis(lower.saturating_add(jitter))
}

fn now_ms() -> anyhow::Result<u64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?;

    Ok(elapsed.as_millis().min(u64::MAX as u128) as u64)
}

fn get_field(entry: &StreamId, name: &str) -> Option<String> {
    entry.get::<String>(name)
}

fn truncate_error(error: &str) -> String {
    const MAX_ERROR_BYTES: usize = 2_048;

    if error.len() <= MAX_ERROR_BYTES {
        return error.to_owned();
    }

    let mut end = MAX_ERROR_BYTES;
    while !error.is_char_boundary(end) {
        end -= 1;
    }

    format!("{}…", &error[..end])
}

pub fn retry_count(entry: &StreamId) -> anyhow::Result<u32> {
    match get_field(entry, "retry_count") {
        None => Ok(0),
        Some(value) => value
            .parse::<u32>()
            .map_err(|error| anyhow::anyhow!("invalid retry_count {value:?}: {error}")),
    }
}

pub async fn transition_failure(
    conn: &mut MultiplexedConnection,
    source_stream: &str,
    group: &str,
    logical_consumer_name: &str,
    entry: &StreamId,
    current_retry_count: u32,
    failure_class: FailureClass,
    error: &str,
    config: &RetryConfig,
) -> anyhow::Result<FailureAction> {
    let stream_id = entry.id.as_str();
    let event_id =
        get_field(entry, "event_id").unwrap_or_else(|| format!("missing-event-id:{stream_id}"));
    let event_type = get_field(entry, "event_type").unwrap_or_else(|| "unknown".to_owned());
    let payload = get_field(entry, "payload").unwrap_or_default();
    let error = truncate_error(error);

    if failure_class == FailureClass::Retryable && current_retry_count < config.max_retries {
        let next_retry_count = current_retry_count.saturating_add(1);
        let delay = retry_delay(next_retry_count, config);
        let due_at_ms = now_ms()?.saturating_add(delay.as_millis() as u64);

        // A deterministic member makes scheduling idempotent for the same
        // logical handler, event, and retry number.
        let member = format!(
            "{}:{}:{}",
            logical_consumer_name, event_id, next_retry_count
        );

        let envelope = RetryEnvelope {
            event_id,
            event_type,
            payload,
            retry_count: next_retry_count,
            source_stream_id: stream_id.to_owned(),
            last_error: error,
        };
        let serialized = serde_json::to_string(&envelope)?;

        let acked: i64 = redis::cmd("EVAL")
            .arg(SCHEDULE_RETRY_SCRIPT)
            .arg(3)
            .arg(source_stream)
            .arg(&config.retry_zset)
            .arg(&config.retry_payloads)
            .arg(group)
            .arg(stream_id)
            .arg(due_at_ms)
            .arg(&member)
            .arg(serialized)
            .query_async(conn)
            .await
            .context("atomic retry scheduling failed")?;

        if acked == 1 {
            debug!(
                stream_id,
                retry_count = next_retry_count,
                due_at_ms,
                "message scheduled for delayed retry"
            );

            return Ok(FailureAction::RetryScheduled {
                retry_count: next_retry_count,
                due_at_ms,
            });
        }

        return Ok(FailureAction::AlreadyHandled);
    }

    let failed_at_ms = now_ms()?;
    let dedupe_field = format!("{source_stream}|{stream_id}");

    let (dead_stream_id, acked, inserted): (String, i64, i64) = redis::cmd("EVAL")
        .arg(DEAD_LETTER_SCRIPT)
        .arg(3)
        .arg(source_stream)
        .arg(&config.dead_stream)
        .arg(&config.dead_index)
        .arg(group)
        .arg(&dedupe_field)
        .arg(stream_id)
        .arg(&event_id)
        .arg(&event_type)
        .arg(&payload)
        .arg(logical_consumer_name)
        .arg(current_retry_count)
        .arg(failure_class.as_str())
        .arg(error)
        .arg(failed_at_ms)
        .query_async(conn)
        .await
        .context("atomic dead-letter transfer failed")?;

    if inserted == 0 && dead_stream_id.is_empty() {
        return Ok(FailureAction::AlreadyHandled);
    }

    if acked == 0 {
        warn!(
            stream_id,
            dead_stream_id, "dead-letter record exists but source entry was no longer pending"
        );
    }

    Ok(FailureAction::DeadLettered { dead_stream_id })
}

/// Publish due retry items back into the source Stream. Each item is transferred
/// by Lua so XADD + ZREM + HDEL cannot be interleaved with another scheduler.
pub async fn publish_due_retries(
    conn: &mut MultiplexedConnection,
    source_stream: &str,
    config: &RetryConfig,
) -> anyhow::Result<usize> {
    let now = now_ms()?;

    let due_members: Vec<String> = redis::cmd("ZRANGEBYSCORE")
        .arg(&config.retry_zset)
        .arg("-inf")
        .arg(now)
        .arg("LIMIT")
        .arg(0)
        .arg(config.scheduler_batch_size)
        .query_async(conn)
        .await
        .context("failed to fetch due retry members")?;

    let mut published = 0;

    for member in due_members {
        let stream_id: Option<String> = redis::cmd("EVAL")
            .arg(PUBLISH_DUE_RETRY_SCRIPT)
            .arg(3)
            .arg(source_stream)
            .arg(&config.retry_zset)
            .arg(&config.retry_payloads)
            .arg(&member)
            .arg(now)
            .query_async(conn)
            .await
            .context("atomic delayed retry publication failed")?;

        if let Some(stream_id) = stream_id {
            published += 1;
            debug!(
                %member,
                %stream_id,
                "delayed retry published to source Stream"
            );
        }
    }

    Ok(published)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::streams::{StreamReadOptions, StreamReadReply};

    fn test_config(suffix: &str) -> RetryConfig {
        RetryConfig {
            retry_zset: format!("mq-rs:{{retry-test}}:{suffix}:retry"),
            retry_payloads: format!("mq-rs:{{retry-test}}:{suffix}:retry:payloads"),
            dead_stream: format!("mq-rs:{{retry-test}}:{suffix}:dead"),
            dead_index: format!("mq-rs:{{retry-test}}:{suffix}:dead:index"),
            max_retries: 2,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            scheduler_interval: Duration::from_millis(10),
            scheduler_batch_size: 10,
        }
    }

    async fn pending_entry(
        conn: &mut MultiplexedConnection,
        stream: &str,
        group: &str,
        consumer: &str,
    ) -> anyhow::Result<StreamId> {
        let _: String = conn.xgroup_create_mkstream(stream, group, "0-0").await?;

        let event = EventEnvelopeForTest::new();
        let _: String = redis::cmd("XADD")
            .arg(stream)
            .arg("*")
            .arg("event_id")
            .arg(&event.event_id)
            .arg("event_type")
            .arg("order.paid")
            .arg("payload")
            .arg(&event.payload)
            .arg("retry_count")
            .arg("0")
            .query_async(conn)
            .await?;

        let options = StreamReadOptions::default().group(group, consumer).count(1);
        let streams = [stream];
        let offsets = [">"];
        let response: Option<StreamReadReply> =
            conn.xread_options(&streams, &offsets, &options).await?;
        response
            .and_then(|reply| reply.keys.into_iter().flat_map(|key| key.ids).next())
            .ok_or_else(|| anyhow::anyhow!("test message was not read into the PEL"))
    }

    struct EventEnvelopeForTest {
        event_id: String,
        payload: String,
    }

    impl EventEnvelopeForTest {
        fn new() -> Self {
            let event_id = Uuid::new_v4().to_string();
            let payload = serde_json::json!({
                "event_id": event_id,
                "event_type": "order.paid",
                "schema_version": 1,
                "occurred_at": "2026-01-01T00:00:00Z",
                "aggregate_type": "order",
                "aggregate_id": "1001",
                "payload": {
                    "order_id": 1001,
                    "buyer_id": 2001,
                    "payment_id": "00000000-0000-0000-0000-000000000001",
                    "amount_cents": 1000,
                    "currency": "CNY"
                },
                "correlation_id": null,
                "causation_id": null
            });
            Self {
                event_id,
                payload: payload.to_string(),
            }
        }
    }

    async fn test_connection() -> anyhow::Result<Option<MultiplexedConnection>> {
        let Ok(url) = std::env::var("TEST_REDIS_URL") else {
            return Ok(None);
        };
        let client = redis::Client::open(url)?;
        Ok(Some(client.get_multiplexed_async_connection().await?))
    }

    #[test]
    fn exponential_backoff_doubles_and_caps() {
        assert_eq!(exponential_delay_ms(1, 1_000, 10_000), 1_000);
        assert_eq!(exponential_delay_ms(2, 1_000, 10_000), 2_000);
        assert_eq!(exponential_delay_ms(3, 1_000, 10_000), 4_000);
        assert_eq!(exponential_delay_ms(4, 1_000, 10_000), 8_000);
        assert_eq!(exponential_delay_ms(5, 1_000, 10_000), 10_000);
        assert_eq!(exponential_delay_ms(0, 1_000, 10_000), 0);
    }

    #[test]
    fn redis_cluster_keys_must_share_a_hash_tag() {
        let config = test_config("hash-test");
        assert!(validate_hash_tags("orders:{retry-test}:main", &config).is_ok());
        assert!(validate_hash_tags("orders:{payments}:main", &config).is_err());
    }

    #[test]
    fn jittered_delay_stays_inside_upper_half_of_backoff_window() {
        let config = RetryConfig {
            base_delay: Duration::from_secs(2),
            max_delay: Duration::from_secs(30),
            ..test_config("jitter")
        };

        for _ in 0..100 {
            let delay = retry_delay(2, &config).as_millis() as u64;
            assert!((2_000..=4_000).contains(&delay));
        }
    }

    #[tokio::test]
    async fn retry_script_schedules_and_scheduler_republishes() -> anyhow::Result<()> {
        let Some(mut conn) = test_connection().await? else {
            return Ok(());
        };

        let suffix = Uuid::new_v4().to_string();
        let stream = format!("mq-rs:{{retry-test}}:{suffix}:main");
        let group = format!("group-{suffix}");
        let consumer = format!("consumer-{suffix}");
        let config = test_config(&suffix);
        validate_hash_tags(&stream, &config)?;

        let entry = pending_entry(&mut conn, &stream, &group, &consumer).await?;
        let event_id = get_field(&entry, "event_id").unwrap();

        let action = transition_failure(
            &mut conn,
            &stream,
            &group,
            "order-payment-projection-v1",
            &entry,
            0,
            FailureClass::Retryable,
            "temporary database connection failure",
            &config,
        )
        .await?;

        assert!(matches!(
            action,
            FailureAction::RetryScheduled { retry_count: 1, .. }
        ));

        let pending: Vec<redis::Value> = redis::cmd("XPENDING")
            .arg(&stream)
            .arg(&group)
            .arg(&entry.id)
            .arg(&entry.id)
            .arg(1)
            .query_async(&mut conn)
            .await?;
        assert!(pending.is_empty());

        let published = publish_due_retries(&mut conn, &stream, &config).await?;
        assert_eq!(published, 1);

        let length: i64 = redis::cmd("XLEN")
            .arg(&stream)
            .query_async(&mut conn)
            .await?;
        assert_eq!(length, 2);

        let retry_count: i64 = redis::cmd("ZCARD")
            .arg(&config.retry_zset)
            .query_async(&mut conn)
            .await?;
        assert_eq!(retry_count, 0);

        let payload_count: i64 = redis::cmd("HLEN")
            .arg(&config.retry_payloads)
            .query_async(&mut conn)
            .await?;
        assert_eq!(payload_count, 0);

        let rows: Vec<redis::Value> = redis::cmd("XRANGE")
            .arg(&stream)
            .arg("-")
            .arg("+")
            .query_async(&mut conn)
            .await?;
        assert_eq!(rows.len(), 2);
        assert!(!event_id.is_empty());

        let keys = [
            stream,
            config.retry_zset,
            config.retry_payloads,
            config.dead_stream,
            config.dead_index,
        ];
        let _: i64 = redis::cmd("DEL")
            .arg(&keys[..])
            .query_async(&mut conn)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn dead_letter_script_moves_and_deduplicates_message() -> anyhow::Result<()> {
        let Some(mut conn) = test_connection().await? else {
            return Ok(());
        };

        let suffix = Uuid::new_v4().to_string();
        let stream = format!("mq-rs:{{retry-test}}:{suffix}:main");
        let group = format!("group-{suffix}");
        let consumer = format!("consumer-{suffix}");
        let config = test_config(&suffix);
        validate_hash_tags(&stream, &config)?;

        let entry = pending_entry(&mut conn, &stream, &group, &consumer).await?;

        let first = transition_failure(
            &mut conn,
            &stream,
            &group,
            "order-payment-projection-v1",
            &entry,
            0,
            FailureClass::Permanent,
            "invalid payment payload",
            &config,
        )
        .await?;

        let first_id = match first {
            FailureAction::DeadLettered { dead_stream_id } => dead_stream_id,
            other => anyhow::bail!("expected DeadLettered, got {other:?}"),
        };

        let second = transition_failure(
            &mut conn,
            &stream,
            &group,
            "order-payment-projection-v1",
            &entry,
            0,
            FailureClass::Permanent,
            "invalid payment payload",
            &config,
        )
        .await?;

        let second_id = match second {
            FailureAction::DeadLettered { dead_stream_id } => dead_stream_id,
            other => anyhow::bail!("expected idempotent DeadLettered, got {other:?}"),
        };

        assert_eq!(first_id, second_id);

        let dead_length: i64 = redis::cmd("XLEN")
            .arg(&config.dead_stream)
            .query_async(&mut conn)
            .await?;
        assert_eq!(dead_length, 1);

        let pending: Vec<redis::Value> = redis::cmd("XPENDING")
            .arg(&stream)
            .arg(&group)
            .arg(&entry.id)
            .arg(&entry.id)
            .arg(1)
            .query_async(&mut conn)
            .await?;
        assert!(pending.is_empty());

        let keys = [
            stream,
            config.retry_zset,
            config.retry_payloads,
            config.dead_stream,
            config.dead_index,
        ];
        let _: i64 = redis::cmd("DEL").arg(&keys).query_async(&mut conn).await?;

        Ok(())
    }
}
