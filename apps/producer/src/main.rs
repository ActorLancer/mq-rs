use anyhow::Context;
use domain::event::EventEnvelope;
use serde_json::json;
use tracing::info;
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // .env is optional: production environments can inject variables directly.
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?),
        )
        .init();

    let redis_url = std::env::var("REDIS_URL").context("REDIS_URL is not configured")?;

    let redis_client = redis::Client::open(redis_url).context("failed to create Redis client")?;

    let mut conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .context("failed to connect to Redis")?;

    // A real producer creates this event in the same business transaction
    // that writes its Outbox row. A retry must reuse this exact event_id,
    // payment_id and payload rather than constructing a new event.
    let event = EventEnvelope::new(
        "order.paid",
        "order",
        "1001",
        json!({
            "order_id": 1001,
            "buyer_id": 2001,
            "payment_id": Uuid::new_v4(),
            "amount_cents": 9999,
            "currency": "CNY"
        }),
    );

    let payload = serde_json::to_string(&event)?;

    let stream_id: String = redis::cmd("XADD")
        .arg("orders:events")
        .arg("*")
        .arg("event_id")
        .arg(event.event_id.to_string())
        .arg("event_type")
        .arg(&event.event_type)
        .arg("payload")
        .arg(payload)
        .query_async(&mut conn)
        .await
        .context("failed to publish event to Redis Stream")?;

    info!(
        event_id = %event.event_id,
        stream_id = %stream_id,
        event_type = %event.event_type,
        "event published"
    );

    Ok(())
}
