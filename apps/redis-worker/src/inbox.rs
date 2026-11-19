use anyhow::{Context, bail};
use chrono::Utc;
use domain::event::EventEnvelope;
use serde::Deserialize;
use sqlx::{PgPool, Row};
use uuid::Uuid;

const CONSUMER_NAME: &str = "order-payment-projection-v1";

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
    // 第一次成功执行了业务更新
    Applied,
    // 相同逻辑消费者已经处理过该 event_id
    DuplicateEvent,
    /// 为什么额外定义 AlreadyApplied？
    /// 因为幂等性有两个层次：
    /// 1. 消息级别幂等：相同 event_id 不重复处理。
    /// 2. 业务级别幂等：同一笔支付不能因为不同事件 ID 而重复产生业务副作用。
    // 不同 event_id，但订单已由同一个 payment_id 完成支付
    AlreadyApplied,
}

pub async fn process_order_paid(
    pool: &PgPool,
    event: &EventEnvelope,
) -> anyhow::Result<ProcessOutcome> {
    // 1. 验证事件类型
    if event.event_type != "order.paid" {
        bail!("unsupported event type: {}", event.event_type);
    }

    // 2. 解析业务 payload
    let payload = serde_json::from_value::<OrderPaidPayload>(event.payload.clone())
        .context("invalid order.paid payload")?;

    // 3. 基础业务验证
    if payload.order_id <= 0 {
        bail!("invalid order_id");
    }

    if payload.amount_cents <= 0 {
        bail!("invalid amount_cents");
    }

    if payload.currency.is_empty() {
        bail!("currency cannot be empty");
    }

    if event.aggregate_type != "order" || event.aggregate_id != payload.order_id.to_string() {
        bail!("event aggregate does not match payload");
    }

    // 4. 开启数据库事务
    let mut tx = pool.begin().await?;

    // 5. 尝试写入 Inbox。
    //
    // 唯一约束：
    // (consumer_name, event_id)
    //
    // 如果已经处理过，不会再次插入。
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
    .bind(CONSUMER_NAME)
    .bind(event.event_id)
    .bind(&event.event_type)
    .bind(&event.aggregate_type)
    .bind(&event.aggregate_id)
    .bind(event.payload.clone())
    .bind(event.occurred_at.clone())
    .fetch_optional(&mut *tx)
    .await?;

    // 6. 如果 Inbox 已经存在，说明这个逻辑消费者
    // 已经成功提交过相同事件。
    if inserted.is_none() {
        tx.commit().await?;

        return Ok(ProcessOutcome::DuplicateEvent);
    }

    // 7. 执行业务更新
    let update = sqlx::query(
        r#"
        UPDATE orders
        SET
            status = 'PAID',
            payment_id = $2,
            paid_amount_cents = $3,
            updated_at = NOW()
        WHERE id = $1
            AND status = 'PENDING'
        "#,
    )
    .bind(payload.order_id)
    .bind(payload.payment_id)
    .bind(payload.amount_cents)
    .execute(&mut *tx)
    .await?;

    if update.rows_affected() == 1 {
        // 8. Inbox 与订单更新同时提交
        tx.commit().await?;

        return Ok(ProcessOutcome::Applied);
    }

    // 9. 更新没有命中 PENDING 状态。
    // 检查是否已经由同一笔支付完成
    let current = sqlx::query(
        // 行级排它锁
        r#"
        SELECT status, payment_id
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
    let payment_id: Option<Uuid> = current.try_get("payment_id")?;

    if status == "PAID" && payment_id == Some(payload.payment_id) {
        // 相同 payment_id 的业务已经成功，
        // 即使 event_id 不同，也不重复执行副作用。
        tx.commit().await?;

        return Ok(ProcessOutcome::AlreadyApplied);
    }

    // 其他状态冲突交由上层进行错误分类，
    // 当前回滚 Inbox 和其他数据库操作。
    tx.rollback().await?;

    bail!(
        "invalid order payment transition: order = {}, status = {}",
        payload.order_id,
        status,
    );
}
