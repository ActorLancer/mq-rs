use anyhow::{Context, Result, anyhow, bail};
use domain::event::EventEnvelope;
use redis::{AsyncCommands, aio::MultiplexedConnection, streams::StreamId};
use sqlx::PgPool;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

use crate::{
    inbox::{ProcessOutcome, process_order_paid},
    retry::{self, FailureAction, FailureClass, RetryConfig},
};

pub const STREAM: &str = "orders:{events}:main";
pub const GROUP: &str = "orders-workers";
const LOGICAL_CONSUMER: &str = "order-payment-projection-v1";

#[derive(Debug, Default)]
pub struct BatchReport {
    pub completed: usize,
    pub acknowledged: usize,
    pub retry_scheduled: usize,
    pub dead_lettered: usize,
    pub no_longer_pending: usize,
    pub failed: usize,
    pub task_panicked: usize,
    pub applied: usize,
    pub duplicate: usize,
    pub already_applied: usize,
}

#[derive(Debug)]
enum EntryAction {
    Processed {
        outcome: ProcessOutcome,
        acknowledged: bool,
    },
    RetryScheduled,
    DeadLettered,
    AlreadyHandled,
}

fn decode_event(entry: &StreamId) -> Result<EventEnvelope> {
    let payload = entry
        .get::<String>("payload")
        .ok_or_else(|| anyhow!("message has no payload field"))?;

    let stored_event_id = entry
        .get::<String>("event_id")
        .ok_or_else(|| anyhow!("message has no event_id field"))?;

    let stored_event_type = entry
        .get::<String>("event_type")
        .ok_or_else(|| anyhow!("message has no event_type field"))?;

    let event: EventEnvelope =
        serde_json::from_str(&payload).context("failed to deserialize event payload")?;

    if event.event_id.to_string() != stored_event_id {
        bail!(
            "event_id mismatch: stream field={}, payload={}",
            stored_event_id,
            event.event_id
        );
    }

    if event.event_type != stored_event_type {
        bail!(
            "event_type mismatch: stream field={}, payload={}",
            stored_event_type,
            event.event_type
        );
    }

    Ok(event)
}

async fn transition_failed_entry(
    ack_conn: &mut MultiplexedConnection,
    entry: &StreamId,
    retry_count: u32,
    failure_class: FailureClass,
    error: &anyhow::Error,
    retry_config: &RetryConfig,
) -> Result<EntryAction> {
    let action = retry::transition_failure(
        ack_conn,
        STREAM,
        GROUP,
        LOGICAL_CONSUMER,
        entry,
        retry_count,
        failure_class,
        &format!("{error:#}"),
        retry_config,
    )
    .await
    .context("failed to schedule retry or move message to DLQ")?;

    match action {
        FailureAction::RetryScheduled {
            retry_count,
            due_at_ms,
        } => {
            warn!(
                stream_id = %entry.id,
                retry_count,
                due_at_ms,
                failure_class = failure_class.as_str(),
                error = %error,
                "message scheduled for delayed retry"
            );
            Ok(EntryAction::RetryScheduled)
        }
        FailureAction::DeadLettered { dead_stream_id } => {
            error!(
                stream_id = %entry.id,
                dead_stream_id = %dead_stream_id,
                retry_count,
                failure_class = failure_class.as_str(),
                error = %error,
                "message moved to dead-letter Stream"
            );
            Ok(EntryAction::DeadLettered)
        }
        FailureAction::AlreadyHandled => {
            info!(
                stream_id = %entry.id,
                "message was already ACKed or transitioned by another worker"
            );
            Ok(EntryAction::AlreadyHandled)
        }
    }
}

/// Execute Inbox + business transaction first. Transient failures are scheduled
/// for delayed retry; permanent failures are transferred to the DLQ. A successful
/// database commit is followed by XACK. The failure-injection path deliberately
/// leaves the entry Pending to verify Inbox idempotency after recovery.
pub async fn process_and_ack(
    pool: &PgPool,
    ack_conn: &mut MultiplexedConnection,
    entry: StreamId,
    fail_before_ack: bool,
    retry_config: &RetryConfig,
) -> Result<EntryAction> {
    let retry_count = match retry::retry_count(&entry) {
        Ok(count) => count,
        Err(error) => {
            return transition_failed_entry(
                ack_conn,
                &entry,
                0,
                FailureClass::Permanent,
                &error,
                retry_config,
            )
            .await;
        }
    };

    let event = match decode_event(&entry) {
        Ok(event) => event,
        Err(error) => {
            return transition_failed_entry(
                ack_conn,
                &entry,
                retry_count,
                FailureClass::Permanent,
                &error,
                retry_config,
            )
            .await;
        }
    };

    let outcome = match process_order_paid(pool, &event, LOGICAL_CONSUMER).await {
        Ok(outcome) => outcome,
        Err(error) => {
            let failure_class = retry::classify_error(&error);
            return transition_failed_entry(
                ack_conn,
                &entry,
                retry_count,
                failure_class,
                &error,
                retry_config,
            )
            .await;
        }
    };

    info!(
        stream_id = %entry.id,
        event_id = %event.event_id,
        ?outcome,
        "database transaction committed"
    );

    if fail_before_ack {
        bail!("fault injected after database COMMIT and before XACK");
    }

    let acked: i64 = ack_conn
        .xack(STREAM, GROUP, &[entry.id.as_str()])
        .await
        .context("XACK failed after database COMMIT")?;

    if acked == 1 {
        info!(
            stream_id = %entry.id,
            event_id = %event.event_id,
            "message ACKed"
        );
        Ok(EntryAction::Processed {
            outcome,
            acknowledged: true,
        })
    } else {
        warn!(
            stream_id = %entry.id,
            event_id = %event.event_id,
            "XACK returned 0; message is no longer pending for this group"
        );
        Ok(EntryAction::AlreadyHandled)
    }
}

/// Keep no more than max_in_flight tasks in JoinSet. This bounds both active
/// work and spawned-but-uncollected tasks. New and XAUTOCLAIMed entries share
/// this exact processing, Inbox, retry, DLQ, and ACK path.
pub async fn process_batch(
    pool: &PgPool,
    ack_connection: &MultiplexedConnection,
    entries: Vec<StreamId>,
    max_in_flight: usize,
    fail_before_ack: bool,
    retry_config: &RetryConfig,
) -> Result<BatchReport> {
    if max_in_flight == 0 {
        bail!("max_in_flight must be greater than zero");
    }

    let mut tasks: JoinSet<(String, Result<EntryAction>)> = JoinSet::new();
    let mut report = BatchReport::default();

    for entry in entries {
        while tasks.len() >= max_in_flight {
            collect_one(&mut tasks, &mut report).await;
        }

        let stream_id = entry.id.clone();
        let pool = pool.clone();
        let mut ack_conn = ack_connection.clone();
        let retry_config = retry_config.clone();

        tasks.spawn(async move {
            let result = process_and_ack(
                &pool,
                &mut ack_conn,
                entry,
                fail_before_ack,
                &retry_config,
            )
            .await;

            (stream_id, result)
        });
    }

    while !tasks.is_empty() {
        collect_one(&mut tasks, &mut report).await;
    }

    Ok(report)
}

async fn collect_one(
    tasks: &mut JoinSet<(String, Result<EntryAction>)>,
    report: &mut BatchReport,
) {
    let Some(joined) = tasks.join_next().await else {
        return;
    };

    match joined {
        Ok((stream_id, Ok(EntryAction::Processed {
            outcome,
            acknowledged,
        }))) => {
            report.completed += 1;
            if acknowledged {
                report.acknowledged += 1;
            }

            match outcome {
                ProcessOutcome::Applied => report.applied += 1,
                ProcessOutcome::DuplicateEvent => report.duplicate += 1,
                ProcessOutcome::AlreadyApplied => report.already_applied += 1,
            }

            info!(
                stream_id = %stream_id,
                ?outcome,
                acknowledged,
                "message processing finished"
            );
        }
        Ok((stream_id, Ok(EntryAction::RetryScheduled))) => {
            report.completed += 1;
            report.retry_scheduled += 1;
            info!(stream_id = %stream_id, "message retry transition completed");
        }
        Ok((stream_id, Ok(EntryAction::DeadLettered))) => {
            report.completed += 1;
            report.dead_lettered += 1;
            info!(stream_id = %stream_id, "message DLQ transition completed");
        }
        Ok((stream_id, Ok(EntryAction::AlreadyHandled))) => {
            report.completed += 1;
            report.no_longer_pending += 1;
            info!(stream_id = %stream_id, "message was already handled");
        }
        Ok((stream_id, Err(error))) => {
            report.failed += 1;
            error!(
                stream_id = %stream_id,
                error = %error,
                "message processing/transition failed; source entry remains Pending"
            );
        }
        Err(join_error) => {
            report.task_panicked += 1;
            error!(
                error = %join_error,
                "message task panicked or was cancelled; source entry may remain Pending"
            );
        }
    }
}
