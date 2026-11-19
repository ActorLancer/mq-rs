mod inbox;
mod processor;
mod recovery;

use anyhow::{Context, anyhow};
use domain::event::EventEnvelope;
use redis::{
    AsyncCommands,
    streams::{StreamId, StreamReadOptions, StreamReadReply},
};
use sqlx::postgres::PgPoolOptions;
use std::{env, time::Duration};
use tracing::{error, info, warn};
use uuid::Uuid;

// TODO: 配置到外置的 toml 文件或者环境变量中
const STREAM: &str = "orders:events";
const GROUP: &str = "orders-workers";
const BATCH_SIZE: usize = 10;
/// 无消息时最大阻塞时间
const BLOCK_MS: usize = 5_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().context("failed to load .env file")?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_line_number(true)
        .with_file(true)
        .init();

    let redis_url = env::var("REDIS_URL").context("REDIS_URL is not configured")?;

    let client = redis::Client::open(redis_url).context("failed to create Redis client")?;

    let database_url = env::var("DATABASE_URL")?;

    let pool = PgPoolOptions::new()
        .max_connections(20)
        .min_connections(2)
        .connect(&database_url)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    // 每个进程生成唯一消费者名称。
    // 生产环境还可以将实例 ID、Pod 名称等纳入命名规则
    let consumer_name =
        env::var("CONSUMER_NAME").unwrap_or_else(|_| format!("worker-{}", Uuid::new_v4()));

    // 故障注入开关
    let fail_before_ack = env::var("FAIL_BEFORE_ACK")
        .map(|value| value == "true")
        .unwrap_or(false);

    // TODO：升级为指数退避 + Jitter
    loop {
        match run_worker(&client, &consumer_name, fail_before_ack).await {
            Ok(()) => break,
            Err(error) => {
                error!(
                    error = %error,
                    "worker stopped unexpectedly; reconnecting in 2 seconds"
                );

                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }

    Ok(())
}

/// 判断 Redis 返回的错误信息是否表示消费者组已存在
fn is_group_already_exists(error: &redis::RedisError) -> bool {
    error.to_string().contains("BUSYGROUP")
}

/// 确保消费者组存在
///
/// 使用 0-0 意味着新创建的消费者组从 Stream 起点开始消费。
/// 使用需要忽略历史消息，值消费之后达到的新消息，可以改成 "$"。
async fn ensure_group(conn: &mut redis::aio::MultiplexedConnection) -> anyhow::Result<()> {
    let result: redis::RedisResult<String> =
        conn.xgroup_create_mkstream(STREAM, GROUP, "0-0").await;

    match result {
        Ok(_) => {
            info!(stream = STREAM, group = GROUP, "consumer group created");
        }
        Err(error) if is_group_already_exists(&error) => {
            info!(
                stream = STREAM,
                group = GROUP,
                "consumer group already exists"
            );
        }
        Err(error) => {
            return Err(error).context("failed to create consumer group");
        }
    }

    Ok(())
}

/// 从 Stream 消息中解析业务事件
fn decode_event(entry: &StreamId) -> anyhow::Result<EventEnvelope> {
    let payload = entry
        .get::<String>("payload")
        .ok_or_else(|| anyhow!("message has no field of payload"))?;

    let stored_event_id: String = entry
        .get("event_id")
        .ok_or_else(|| anyhow!("message has no field of event_id"))?;

    let event: EventEnvelope =
        serde_json::from_str(&payload).context("failed to deserialize event payload")?;

    // 检查 Stream 字段和业务事件内部 ID 是否一致
    if event.event_id.to_string() != stored_event_id {
        return Err(anyhow!(
            "event_id mismatch: stream field={}, payload={}",
            stored_event_id,
            event.event_id
        ));
    }

    Ok(event)
}

/// 启动一个 Worker
///
/// 出现 Redis 网络错误时返回 Err，由外层重新连接
async fn run_worker(
    client: &redis::Client,
    consumer_name: &str,
    fail_before_ack: bool,
) -> anyhow::Result<()> {
    // 每个 Worker 使用独立的连接。
    // 该连接用于 BLOCK 读取，不与其他业务任务共享。
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("failed to connect to Redis")?;

    ensure_group(&mut conn).await?;

    info!(
        consumer = consumer_name,
        stream = STREAM,
        group = GROUP,
        "worker started"
    );

    // 创建消费者读取选项
    let options = StreamReadOptions::default()
        .group(GROUP, consumer_name)
        .count(BATCH_SIZE)
        .block(BLOCK_MS);

    loop {
        // > 表示读取消费者组中尚未投递的消息。
        //
        // BLOCK 超时且没有消息时，返回 None。
        let reply: Option<StreamReadReply> = conn
            .xread_options(&[STREAM], &[">"], &options)
            .await
            .context("XREADGROUP failed")?;

        let Some(reply) = reply else {
            continue;
        };

        for stream in reply.keys {
            for entry in stream.ids {
                let stream_id = entry.id.clone();

                // 解析业务事件
                let event = match decode_event(&entry) {
                    Ok(event) => event,
                    Err(error) => {
                        error!(
                            stream_id = %stream_id,
                            error = %error,
                            "failed to decode message; leaving it pending"
                        );

                        // 不能因为解释失败就直接 ACK。
                        // 当前保留在 PEl，后续实现 DLQ。
                        continue;
                    }
                };

                info!(
                    stream_id = %stream_id,
                    event_id = %&event.event_id,
                    event_type = %&event.event_type,
                    aggregate_id = %&event.aggregate_id,
                    stream_key = %&stream.key,
                    "message received"
                );

                // 模拟业务处理耗时。
                //
                // 真实系统中这里可能是：
                // 1. 查询数据库
                // 2. 更新订单
                // 3. 生成授权
                // 4. 调用下游服务
                tokio::time::sleep(Duration::from_millis(150)).await;

                // 故障测试模式
                // 故意不 ACK，观察 PEL
                if fail_before_ack {
                    warn!(
                        stream_id = %stream_id,
                        event_id = %&event.event_id,
                        "test mode: leaving message pending"
                    );

                    continue;
                }

                // 只有业务处理成功才 ACK
                let id = [stream_id.as_str()];

                let acked: i64 = conn.xack(STREAM, GROUP, &id).await.context("XACK failed")?;

                if acked == 1 {
                    info!(
                        stream_id = %stream_id,
                        event_id = %event.event_id,
                        "message acknowledged"
                    );
                } else {
                    warn!(
                        stream_id = %stream_id,
                        event_id = %&event.event_id,
                        "message was not acknowledged"
                    );
                }
            }
        }
    }
}
