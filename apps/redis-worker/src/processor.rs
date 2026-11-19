use anyhow::{Context, Result, anyhow, bail};
use domain::event::EventEnvelope;
use redis::{AsyncCommands, aio::MultiplexedConnection, streams::StreamId};
use sqlx::PgPool;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

use crate::inbox::{ProcessOutcome, process_order_paid};

pub const STREAM: &str = "orders:events";
pub const GROUP: &str = "orders-workers";
const LOGICAL_CONSUMER: &str = "order-payment-projection-v1";

#[derive(Debug)]
pub struct ProcessResult {
    pub outcome: ProcessOutcome,
    pub acknowledged: bool,
}

#[derive(Debug, Default)]
pub struct BatchReport {
    pub acknowledged: usize,
    pub completed: usize,
    pub failed: usize,
    pub task_panicked: usize,
    pub applied: usize,
    pub duplicate: usize,
    pub already_applied: usize,
}

fn decode_event(entry: &StreamId) -> Result<EventEnvelope> {
    let payload = entry
        .get::<String>("payload")
        .ok_or_else(|| anyhow!("message has no payload field"))?;

    let stored_event_id = entry
        .get::<String>("event_id")
        .ok_or_else(|| anyhow!("message has no event_id field"))?;

    let event: EventEnvelope =
        serde_json::from_str(&payload).context("failed to deserialize event payload")?;

    if event.event_id.to_string() != stored_event_id {
        bail!(
            "event_id mismatch: stream field={}, payload={}",
            stored_event_id,
            event.event_id
        );
    }

    Ok(event)
}

/// Execute the Inbox transaction first and ACK only after it commits.
pub async fn process_and_ack(
    pool: &PgPool,
    ack_conn: &mut MultiplexedConnection,
    entry: StreamId,
    fail_before_ack: bool,
) -> Result<ProcessResult> {
    let stream_id = entry.id.clone();
    let event = decode_event(&entry)?;

    let outcome = process_order_paid(pool, &event, LOGICAL_CONSUMER)
        .await
        .context("Inbox/business transaction failed")?;

    info!(
        stream_id = %stream_id,
        event_id = %event.event_id,
        ?outcome,
        "database transaction committed"
    );

    if fail_before_ack {
        bail!("fault injected after database COMMIT and before XACK");
    }

    let acked: i64 = ack_conn
        .xack(STREAM, GROUP, &[stream_id.as_str()])
        .await
        .context("XACK failed after database COMMIT")?;

    if acked == 1 {
        info!(stream_id = %stream_id, event_id = %event.event_id, "message ACKed");
    } else {
        warn!(
            stream_id = %stream_id,
            event_id = %event.event_id,
            "XACK returned 0; message is no longer pending for this group"
        );
    }

    Ok(ProcessResult {
        outcome,
        acknowledged: acked == 1,
    })
}

/// Keep no more than max_in_flight tasks in JoinSet. This bounds both active
/// work and the number of spawned-but-uncollected tasks.
pub async fn process_batch(
    pool: &PgPool,
    ack_connection: &MultiplexedConnection,
    entries: Vec<StreamId>,
    max_in_flight: usize,
    fail_before_ack: bool,
) -> Result<BatchReport> {
    if max_in_flight == 0 {
        bail!("max_in_flight must be greater than zero");
    }

    let mut tasks: JoinSet<(String, Result<ProcessResult>)> = JoinSet::new();
    let mut report = BatchReport::default();

    for entry in entries {
        while tasks.len() >= max_in_flight {
            collect_one(&mut tasks, &mut report).await;
        }

        let stream_id = entry.id.clone();
        let pool = pool.clone();
        let mut ack_conn = ack_connection.clone();

        tasks.spawn(async move {
            let result = process_and_ack(&pool, &mut ack_conn, entry, fail_before_ack).await;

            (stream_id, result)
        });
    }

    while !tasks.is_empty() {
        collect_one(&mut tasks, &mut report).await;
    }

    Ok(report)
}

async fn collect_one(
    tasks: &mut JoinSet<(String, Result<ProcessResult>)>,
    report: &mut BatchReport,
) {
    let Some(joined) = tasks.join_next().await else {
        return;
    };

    match joined {
        Ok((stream_id, Ok(result))) => {
            report.completed += 1;
            if result.acknowledged {
                report.acknowledged += 1;
            }

            match result.outcome {
                ProcessOutcome::Applied => report.applied += 1,
                ProcessOutcome::DuplicateEvent => report.duplicate += 1,
                ProcessOutcome::AlreadyApplied => report.already_applied += 1,
            }

            info!(
                stream_id = %stream_id,
                outcome = ?result.outcome,
                acknowledged = result.acknowledged,
                "message processing finished"
            );
        }
        Ok((stream_id, Err(error))) => {
            report.failed += 1;
            error!(
                stream_id = %stream_id,
                error = %error,
                "message processing failed; message remains pending"
            );
        }
        Err(join_error) => {
            report.task_panicked += 1;
            error!(
                error = %join_error,
                "message task panicked or was cancelled; message may remain pending"
            );
        }
    }
}
