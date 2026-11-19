use anyhow::Context;
use redis::{
    AsyncCommands, Value, from_redis_value_ref,
    streams::{StreamAutoClaimOptions, StreamAutoClaimReply},
};
use tracing::{info, warn};

#[derive(Debug)]
pub struct ClaimedBatch {
    pub next_cursor: String,

    /// 已经接管的消息
    pub entries: Vec<ClaimedEntry>,

    /// 已经从 Stream 删除但仍残留于 PEL 的 ID
    pub deleted_ids: Vec<String>,
}

#[derive(Debug)]
pub struct ClaimedEntry {
    pub stream_id: String,
    pub event_id: Option<String>,
    pub event_type: Option<String>,
    pub payload: Option<String>,
}

// 将 Redis 消息字段解析为结构化结果。
//
// Redis 7.0+ 的结果结构可以理解为：
//
// [
//     next_cursor,
//     [
//         [
//             stream_id,
//             [field1, value1, field2, value2, ...]
//         ],
//         ...
//     ],
//     [
//         deleted_id_1,
//         deleted_id_2
//     ]
// ]
fn parse_xautoclaim(value: Value) -> redis::RedisResult<ClaimedBatch> {
    let outer: Vec<Value> = from_redis_value_ref(&value)?;

    if outer.len() < 2 {
        return Err(redis::RedisError::from((
            redis::ErrorKind::UnexpectedReturnType,
            "invalid XAUTOCLAIM response",
        )));
    }

    let next_cursor: String = from_redis_value_ref(&outer[0])?;

    let raw_entries: Vec<Value> = from_redis_value_ref(&outer[1])?;

    let mut entries = Vec::new();

    for raw_entry in raw_entries {
        let parts: Vec<Value> = from_redis_value_ref(&raw_entry)?;

        if parts.len() != 2 {
            continue;
        }

        let stream_id: String = from_redis_value_ref(&parts[0])?;
        let fields: Vec<String> = from_redis_value_ref(&parts[1])?;

        let mut event_id = None;
        let mut event_type = None;
        let mut payload = None;

        for pair in fields.chunks_exact(2) {
            match pair[0].as_str() {
                "event_id" => event_id = Some(pair[1].clone()),
                "event_type" => event_type = Some(pair[1].clone()),
                "payload" => payload = Some(pair[1].clone()),
                _ => {}
            }
        }

        entries.push(ClaimedEntry {
            stream_id,
            event_id,
            event_type,
            payload,
        });
    }

    let deleted_ids: Vec<String> = if outer.len() >= 3 {
        from_redis_value_ref(&outer[2])?
    } else {
        Vec::new()
    };

    Ok(ClaimedBatch {
        next_cursor,
        entries,
        deleted_ids,
    })
}

fn parse_xautoclaim_v2(value: StreamAutoClaimReply) -> redis::RedisResult<ClaimedBatch> {
    let mut claimed_entries = Vec::new();

    for entry in value.claimed {
        let event_id = entry.get::<String>("event_id");
        let event_type = entry.get::<String>("event_type");
        let payload = entry.get::<String>("payload");

        claimed_entries.push(ClaimedEntry {
            event_id,
            event_type,
            payload,
            stream_id: entry.id,
        });
    }

    Ok(ClaimedBatch {
        next_cursor: value.next_stream_id,
        entries: claimed_entries,
        deleted_ids: value.deleted_ids,
    })
}

pub async fn claim_pending(
    conn: &mut redis::aio::MultiplexedConnection,
    stream: &str,
    group: &str,
    consumer: &str,
    min_idle_ms: u64,
    cursor: &str,
    count: usize,
) -> redis::RedisResult<ClaimedBatch> {
    let options = StreamAutoClaimOptions::default().count(count);

    let response: StreamAutoClaimReply = conn
        .xautoclaim_options(stream, group, consumer, min_idle_ms, cursor, options)
        .await?;

    parse_xautoclaim_v2(response)
}

// Pending 恢复扫描器
pub async fn recover_pending(
    conn: &mut redis::aio::MultiplexedConnection,
    stream: &str,
    group: &str,
    consumer: &str,
    min_idle_ms: u64,
) -> anyhow::Result<()> {
    let mut cursor = "0-0".to_string();

    loop {
        let batch = claim_pending(conn, stream, group, consumer, min_idle_ms, &cursor, 100)
            .await
            .context("failed to execute XAUTOCLAIM")?;

        info!(
            next_cursor = %&batch.next_cursor,
            claimed = batch.entries.len(),
            deleted = batch.deleted_ids.len(),
            "pending recovery scan"
        );

        // Stream 消息已经被删除，但 PEL 还留有引用。
        // 记录下来，便于排查和监控
        for deleted_id in &batch.deleted_ids {
            warn!(
                stream_id = %deleted_id,
                "dangling PEL entry: original stream message is missing"
            );
        }

        for entry in batch.entries {
            info!(
                stream_id = %&entry.stream_id,
                event_id = ?&entry.event_id,
                event_type = ?&entry.event_type,
                "pending message claimed"
            );

            // TODO:
            // 下一步接入业务处理与 XACK。
        }

        if batch.next_cursor == "0-0" {
            break Ok(());
        }

        cursor = batch.next_cursor;
    }
}
