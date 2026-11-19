use anyhow::Context;
use domain::event::EventEnvelope;
use serde_json::json;
use tracing::info;
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().context("Failed to load .env file")?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?),
        )
        .init();

    let redis_url = std::env::var("REDIS_URL").context("REDIS_URL is not configured")?;

    let redis_client = redis::Client::open(redis_url).context("failed to create Redis client")?;

    let mut client = redis_client
        .get_multiplexed_async_connection()
        .await
        .context("failed to connect to Redis")?;

    // 注意：
    //
    // 事件创建时生成 event_id，发布重试时复用原事件及其 event_id
    //
    // 这个 Demo 中每次运行 Producer 都会创建一个新的事件 ID 和 payment_id。
    //
    // 在实际 Outbox 模式中，支付业务创建事件时就要生成并持久化这两个 ID。
    // 发布重试必须从 Outbox 读取原记录，不能重新生成。
    let event = EventEnvelope::new(
        "order.paid",
        "order",
        "1001",
        json!({
            "order_id": 1001,
            "buyer_id": 2001,
            "payment_id": Uuid::new_v4(),
            "amount": 9999,
            "currency": "CNY"
        }),
    );

    let payload = serde_json::to_string(&event)?;

    let stream_id = redis::cmd("XADD")
        .arg("orders:events")
        .arg("*")
        .arg("event_id")
        .arg(event.event_id.to_string())
        .arg("event_type")
        .arg(&event.event_type)
        .arg("payload")
        .arg(payload)
        .query_async::<String>(&mut client)
        .await
        .context("failed to publish event to Redis Stream")?;

    info!(
        event_id = %&event.event_id,
        stream_id = %stream_id,
        event_type = %event.event_type,
        "event published"
    );

    Ok(())
}
