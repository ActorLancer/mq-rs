use domain::event::EventEnvelope;
use redis_worker::{
    MIGRATOR,
    inbox::{ProcessOutcome, process_order_paid},
};
use serde_json::json;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::env;
use uuid::Uuid;

async fn test_pool() -> PgPool {
    let workspace_env = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.env");
    let _ = dotenvy::from_path(workspace_env);

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must point to a disposable PostgreSQL test database");

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .expect("connect to PostgreSQL test database");

    MIGRATOR
        .run(&pool)
        .await
        .expect("apply migrations to PostgreSQL test database");

    pool
}

fn unique_order_id() -> i64 {
    let value = (Uuid::new_v4().as_u128() as i64) & i64::MAX;
    value.max(1)
}

fn paid_event(order_id: i64, buyer_id: i64, payment_id: Uuid) -> EventEnvelope {
    EventEnvelope::new(
        "order.paid",
        "order",
        order_id.to_string(),
        json!({
            "order_id": order_id,
            "buyer_id": buyer_id,
            "payment_id": payment_id,
            "amount_cents": 12_345,
            "currency": "CNY"
        }),
    )
}

async fn insert_pending_order(pool: &PgPool, order_id: i64, buyer_id: i64) {
    sqlx::query(
        r#"
        INSERT INTO orders (id, status, buyer_id, currency)
        VALUES ($1, 'PENDING', $2, 'CNY')
        "#,
    )
    .bind(order_id)
    .bind(buyer_id)
    .execute(pool)
    .await
    .expect("insert test order");
}

async fn cleanup(pool: &PgPool, consumer_name: &str, order_id: i64) {
    sqlx::query("DELETE FROM inbox_messages WHERE consumer_name = $1")
        .bind(consumer_name)
        .execute(pool)
        .await
        .expect("delete test Inbox rows");

    sqlx::query("DELETE FROM orders WHERE id = $1")
        .bind(order_id)
        .execute(pool)
        .await
        .expect("delete test order");
}

#[tokio::test]
async fn inbox_is_idempotent_and_detects_event_id_collisions() {
    let pool = test_pool().await;
    let order_id = unique_order_id();
    let buyer_id = unique_order_id();
    let payment_id = Uuid::new_v4();
    let consumer_name = format!("integration-test-{}", Uuid::new_v4());

    insert_pending_order(&pool, order_id, buyer_id).await;

    let event = paid_event(order_id, buyer_id, payment_id);

    let first = process_order_paid(&pool, &event, &consumer_name)
        .await
        .expect("first delivery should succeed");
    assert!(matches!(first, ProcessOutcome::Applied));

    let replay = process_order_paid(&pool, &event, &consumer_name)
        .await
        .expect("redelivery should be accepted as a duplicate");
    assert!(matches!(replay, ProcessOutcome::DuplicateEvent));

    // A reused event_id with a different payload is a producer contract
    // violation. It must not be silently treated as a harmless duplicate.
    let mut collision = event.clone();
    collision.payload["amount_cents"] = json!(99);
    let collision_result = process_order_paid(&pool, &collision, &consumer_name).await;
    assert!(collision_result.is_err());

    // A different event_id for the very same payment must not apply the
    // business transition a second time.
    let same_payment_new_event = paid_event(order_id, buyer_id, payment_id);
    let already_applied = process_order_paid(&pool, &same_payment_new_event, &consumer_name)
        .await
        .expect("same payment under a new event id should be recognized");
    assert!(matches!(already_applied, ProcessOutcome::AlreadyApplied));

    let (status, stored_payment_id, amount): (String, Option<Uuid>, Option<i64>) =
        sqlx::query_as("SELECT status, payment_id, paid_amount_cents FROM orders WHERE id = $1")
            .bind(order_id)
            .fetch_one(&pool)
            .await
            .expect("read updated order");

    assert_eq!(status, "PAID");
    assert_eq!(stored_payment_id, Some(payment_id));
    assert_eq!(amount, Some(12_345));

    let inbox_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM inbox_messages WHERE consumer_name = $1")
            .bind(&consumer_name)
            .fetch_one(&pool)
            .await
            .expect("count Inbox rows");

    // The original event and the distinct event_id for the same payment
    // are recorded. The replay and colliding payload create no new row.
    assert_eq!(inbox_count, 2);

    cleanup(&pool, &consumer_name, order_id).await;
    pool.close().await;
}

#[tokio::test]
async fn concurrent_redelivery_applies_the_database_side_effect_once() {
    let pool = test_pool().await;
    let order_id = unique_order_id();
    let buyer_id = unique_order_id();
    let payment_id = Uuid::new_v4();
    let consumer_name = format!("concurrent-test-{}", Uuid::new_v4());

    insert_pending_order(&pool, order_id, buyer_id).await;
    let event = paid_event(order_id, buyer_id, payment_id);

    let (left, right) = tokio::join!(
        process_order_paid(&pool, &event, &consumer_name),
        process_order_paid(&pool, &event, &consumer_name),
    );

    let outcomes = [
        left.expect("first concurrent delivery"),
        right.expect("second concurrent delivery"),
    ];

    let applied = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, ProcessOutcome::Applied))
        .count();
    let duplicates = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, ProcessOutcome::DuplicateEvent))
        .count();

    assert_eq!(applied, 1);
    assert_eq!(duplicates, 1);

    let inbox_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inbox_messages WHERE consumer_name = $1 AND event_id = $2",
    )
    .bind(&consumer_name)
    .bind(event.event_id)
    .fetch_one(&pool)
    .await
    .expect("count concurrent Inbox record");

    assert_eq!(inbox_count, 1);

    cleanup(&pool, &consumer_name, order_id).await;
    pool.close().await;
}

#[tokio::test]
async fn failed_business_transition_rolls_back_the_inbox_insert() {
    let pool = test_pool().await;
    let missing_order_id = unique_order_id();
    let buyer_id = unique_order_id();
    let consumer_name = format!("rollback-test-{}", Uuid::new_v4());
    let event = paid_event(missing_order_id, buyer_id, Uuid::new_v4());

    let result = process_order_paid(&pool, &event, &consumer_name).await;
    assert!(result.is_err());

    let inbox_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inbox_messages WHERE consumer_name = $1 AND event_id = $2",
    )
    .bind(&consumer_name)
    .bind(event.event_id)
    .fetch_one(&pool)
    .await
    .expect("count rolled-back Inbox record");

    assert_eq!(inbox_count, 0);
    pool.close().await;
}
