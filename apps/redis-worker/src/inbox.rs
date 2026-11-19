use anyhow::{Context, bail};
use domain::event::EventEnvelope;
use serde::Deserialize;
use sqlx::{PgPool, Row};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct OrderPaidPayload {
    pub order_id: i64,
    pub buyer_id: i64,
    pub payment_id: Uuid,
    pub amount_cents: i64,
    pub currency: String,
}

#[derive(Debug, Clone, Copy)]
pub enum ProcessOutcome {
    Applied,
    DuplicateEvent,
    AlreadyApplied,
}

/// Apply an order.paid event exactly once with respect to this logical
/// consumer's PostgreSQL side effects. Inbox insertion and order mutation
/// share one transaction; Redis XACK is deliberately performed afterwards.
pub async fn process_order_paid(
    pool: &PgPool,
    event: &EventEnvelope,
    consumer_name: &str,
) -> anyhow::Result<ProcessOutcome> {
    if event.event_type != "order.paid" {
        bail!("unsupported event type: {}", event.event_type);
    }

    if event.schema_version != 1 {
        bail!(
            "unsupported order.paid schema version: {}",
            event.schema_version
        );
    }

    let payload: OrderPaidPayload =
        serde_json::from_value(event.payload.clone()).context("invalid order.paid payload")?;

    if payload.order_id <= 0 {
        bail!("invalid order_id");
    }
    if payload.buyer_id <= 0 {
        bail!("invalid buyer_id");
    }
    if payload.amount_cents <= 0 {
        bail!("invalid amount_cents");
    }
    if payload.currency.len() != 3
        || !payload
            .currency
            .bytes()
            .all(|byte| byte.is_ascii_uppercase())
    {
        bail!("currency must be a three-letter uppercase code");
    }
    if event.aggregate_type != "order" || event.aggregate_id != payload.order_id.to_string() {
        bail!("event aggregate does not match order.paid payload");
    }

    let mut tx = pool.begin().await?;

    let inserted: Option<Uuid> = sqlx::query_scalar(
        r#"
        INSERT INTO inbox_messages (
            consumer_name,
            event_id,
            event_type,
            aggregate_type,
            aggregate_id,
            payload,
            occurred_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (consumer_name, event_id)
        DO NOTHING
        RETURNING event_id
        "#,
    )
    .bind(consumer_name)
    .bind(event.event_id)
    .bind(&event.event_type)
    .bind(&event.aggregate_type)
    .bind(&event.aggregate_id)
    .bind(event.payload.clone())
    .bind(event.occurred_at)
    .fetch_optional(&mut *tx)
    .await?;

    if inserted.is_none() {
        // Reusing an event_id with a different body is a producer-side
        // contract violation, not a harmless duplicate.
        let existing = sqlx::query(
            r#"
            SELECT event_type, aggregate_type, aggregate_id, payload
            FROM inbox_messages
            WHERE consumer_name = $1 AND event_id = $2
            "#,
        )
        .bind(consumer_name)
        .bind(event.event_id)
        .fetch_one(&mut *tx)
        .await?;

        let existing_event_type: String = existing.try_get("event_type")?;
        let existing_aggregate_type: String = existing.try_get("aggregate_type")?;
        let existing_aggregate_id: String = existing.try_get("aggregate_id")?;
        let existing_payload: serde_json::Value = existing.try_get("payload")?;

        let same_event = existing_event_type == event.event_type
            && existing_aggregate_type == event.aggregate_type
            && existing_aggregate_id == event.aggregate_id
            && existing_payload == event.payload;

        if !same_event {
            tx.rollback().await?;
            bail!(
                "event_id collision: event {} was previously stored with different content",
                event.event_id
            );
        }

        tx.commit().await?;
        return Ok(ProcessOutcome::DuplicateEvent);
    }

    let update = sqlx::query(
        r#"
        UPDATE orders
        SET
            status = 'PAID',
            payment_id = $3,
            paid_amount_cents = $4,
            updated_at = NOW()
        WHERE id = $1
          AND buyer_id = $2
          AND currency = $5
          AND status = 'PENDING'
        "#,
    )
    .bind(payload.order_id)
    .bind(payload.buyer_id)
    .bind(payload.payment_id)
    .bind(payload.amount_cents)
    .bind(&payload.currency)
    .execute(&mut *tx)
    .await?;

    if update.rows_affected() == 1 {
        tx.commit().await?;
        return Ok(ProcessOutcome::Applied);
    }

    let current = sqlx::query(
        r#"
        SELECT
            status,
            buyer_id,
            currency,
            payment_id,
            paid_amount_cents
        FROM orders
        WHERE id = $1
        FOR UPDATE
        "#,
    )
    .bind(payload.order_id)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(current) = current else {
        tx.rollback().await?;
        bail!("order not found: {}", payload.order_id);
    };

    let status: String = current.try_get("status")?;
    let buyer_id: i64 = current.try_get("buyer_id")?;
    let currency: String = current.try_get("currency")?;
    let payment_id: Option<Uuid> = current.try_get("payment_id")?;
    let paid_amount_cents: Option<i64> = current.try_get("paid_amount_cents")?;

    let same_payment = status == "PAID"
        && buyer_id == payload.buyer_id
        && currency == payload.currency
        && payment_id == Some(payload.payment_id)
        && paid_amount_cents == Some(payload.amount_cents);

    if same_payment {
        tx.commit().await?;
        return Ok(ProcessOutcome::AlreadyApplied);
    }

    tx.rollback().await?;
    bail!(
        "invalid order payment transition: order={}, status={}",
        payload.order_id,
        status
    );
}
