use anyhow::Context;
use redis::{
    AsyncCommands,
    aio::MultiplexedConnection,
    streams::{StreamAutoClaimOptions, StreamAutoClaimReply},
};
use sqlx::PgPool;
use tracing::warn;

use crate::{
    processor::{BatchReport, GROUP, STREAM, process_batch},
    retry::RetryConfig,
};

#[derive(Debug, Default)]
pub struct RecoveryReport {
    pub scanned_batches: usize,
    pub claimed: usize,
    pub deleted: usize,
    pub failed: usize,
    pub acknowledged: usize,
    pub retry_scheduled: usize,
    pub dead_lettered: usize,
}

/// Scan the whole PEL using Redis' returned cursor. Claimed messages go
/// through the exact same processor -> Inbox/retry/DLQ/ACK path as new
/// messages; recovery is not a separate business-processing implementation.
pub async fn recover_pending(
    claim_conn: &mut MultiplexedConnection,
    pool: &PgPool,
    ack_conn: &MultiplexedConnection,
    consumer_name: &str,
    min_idle_ms: u64,
    max_in_flight: usize,
    fail_before_ack: bool,
    retry_config: &RetryConfig,
) -> anyhow::Result<RecoveryReport> {
    let mut cursor = "0-0".to_owned();
    let mut report = RecoveryReport::default();

    loop {
        let options = StreamAutoClaimOptions::default().count(100);

        let reply: StreamAutoClaimReply = claim_conn
            .xautoclaim_options(STREAM, GROUP, consumer_name, min_idle_ms, &cursor, options)
            .await
            .context("XAUTOCLAIM failed")?;

        report.scanned_batches += 1;
        report.claimed += reply.claimed.len();
        report.deleted += reply.deleted_ids.len();

        for deleted_id in &reply.deleted_ids {
            warn!(
                stream_id = %deleted_id,
                "XAUTOCLAIM removed a dangling PEL reference to a deleted stream entry"
            );
        }

        let next_cursor = reply.next_stream_id;

        if !reply.claimed.is_empty() {
            let batch: BatchReport = process_batch(
                pool,
                ack_conn,
                reply.claimed,
                max_in_flight,
                fail_before_ack,
                retry_config,
            )
            .await
            .context("failed to process claimed Pending messages")?;

            report.acknowledged += batch.acknowledged;
            report.failed += batch.failed + batch.task_panicked;
            report.retry_scheduled += batch.retry_scheduled;
            report.dead_lettered += batch.dead_lettered;
        }

        if next_cursor == "0-0" {
            break;
        }

        cursor = next_cursor;
    }

    Ok(report)
}
