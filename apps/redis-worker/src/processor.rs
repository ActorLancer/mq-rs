use anyhow::{Context, Result};
use domain::event::EventEnvelope;
use redis::{AsyncCommands, streams::StreamId};
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::{error, info};

use crate::inbox::process_order_paid;

const STREAM: &str = "orders:events";
const GROUP: &str = "orders-workers";

pub async fn process_and_ack(
    pool: &PgPool,
    ack_conn: &mut redis::aio::MultiplexedConnection,
    entry: &StreamId,
) -> anyhow::Result<()> {
    let stream_id = entry.id.clone();

    let payload: String = entry
        .get("payload")
        .ok_or_else(|| anyhow::anyhow!("missing payload"))?;

    let event_id: String = entry
        .get("event_id")
        .ok_or_else(|| anyhow::anyhow!("missing event_id"))?;

    let event: domain::event::EventEnvelope = serde_json::from_str(&payload)?;

    if event.event_id.to_string() != event_id {
        anyhow::bail!("event_id mismatch");
    }

    // 先提交 PostgreSQL 事务
    let outcome = process_order_paid(pool, &event)
        .await
        .context("inbox processing failed")?;

    info!(
        event_id = %&event.event_id,
        ?outcome,
        "database processing completed"
    );

    // 事务成功后再 ACK
    let acked: i64 = ack_conn
        .xack(STREAM, GROUP, &[stream_id.as_str()])
        .await
        .context("XACK failed after database commit")?;

    if acked == 1 {
        info!(
            stream_id = %stream_id,
            "message acknowledged"
        );
    } else {
        info!(
            stream_id = %stream_id,
            "message was already acknowledged or no longer pending"
        );
    }

    Ok(())
}

// 生产环境可以进一步改为：
// - 读取一批消息。
// - 控制 JoinSet::len()。
// - 达到并发上限时先等待任务完成。
// - 完成后再提交下一批任务。
pub async fn process_batch(
    events: Vec<(String, EventEnvelope)>,
    max_concurrency: usize,
) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(max_concurrency));

    let mut tasks = JoinSet::new();

    for (stream_id, event) in events {
        let permit = semaphore.clone().acquire_owned().await?;

        tasks.spawn(async move {
            // permit 会在任务结束时自动释放
            let _permit = permit;
            process_one(stream_id, event).await
        });
    }

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => info!("message processing completed"),
            Ok(Err(error)) => {
                error!(
                    error = %error,
                    "message processing failed"
                );
            }
            Err(join_error) => {
                error!(
                    error = %join_error,
                    "message task panicked or was cancelled"
                );
            }
        }
    }

    Ok(())
}

async fn process_one(stream_id: String, event: EventEnvelope) -> Result<()> {
    info!(
        stream_id = %stream_id,
        event_id = %event.event_id,
        event_type = %event.event_type,
        "processing event"
    );

    // 模拟耗时业务。
    tokio::time::sleep(Duration::from_millis(200)).await;

    info!(
        stream_id = %stream_id,
        event_id = %event.event_id,
        "business processing succeeded"
    );

    Ok(())
}
