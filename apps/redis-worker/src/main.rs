use anyhow::Context;
use redis::{
    AsyncCommands,
    streams::{StreamReadOptions, StreamReadReply},
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{env, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use redis_worker::{
    migrate,
    processor::{GROUP, STREAM, process_batch},
    recovery::recover_pending,
    retry::{self, RetryConfig},
};

const READ_BATCH_SIZE: usize = 10;
const READ_BLOCK_MS: usize = 5_000;
const DEFAULT_MAX_IN_FLIGHT: usize = 10;
const DEFAULT_MIN_IDLE_MS: u64 = 60_000;
const DEFAULT_RECOVERY_INTERVAL_SECS: u64 = 5;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_line_number(true)
        .with_file(true)
        .init();

    let redis_url = env::var("REDIS_URL").context("REDIS_URL is not configured")?;
    let database_url = env::var("DATABASE_URL").context("DATABASE_URL is not configured")?;

    let max_in_flight = parse_env("MAX_IN_FLIGHT", DEFAULT_MAX_IN_FLIGHT)?;
    let min_idle_ms = parse_env("MIN_IDLE_MS", DEFAULT_MIN_IDLE_MS)?;
    let recovery_interval_secs =
        parse_env("RECOVERY_INTERVAL_SECS", DEFAULT_RECOVERY_INTERVAL_SECS)?;

    anyhow::ensure!(max_in_flight > 0, "MAX_IN_FLIGHT must be greater than zero");
    anyhow::ensure!(
        recovery_interval_secs > 0,
        "RECOVERY_INTERVAL_SECS must be greater than zero"
    );

    let fail_before_ack = env::var("FAIL_BEFORE_ACK")
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let retry_config = RetryConfig {
        retry_zset: env::var("RETRY_ZSET")
            .unwrap_or_else(|_| "orders:{events}:retry".to_owned()),
        retry_payloads: env::var("RETRY_PAYLOADS")
            .unwrap_or_else(|_| "orders:{events}:retry:payloads".to_owned()),
        dead_stream: env::var("DLQ_STREAM")
            .unwrap_or_else(|_| "orders:{events}:dead".to_owned()),
        dead_index: env::var("DLQ_INDEX")
            .unwrap_or_else(|_| "orders:{events}:dead:index".to_owned()),
        max_retries: parse_env("MAX_RETRIES", 5_u32)?,
        base_delay: Duration::from_millis(parse_env("RETRY_BASE_DELAY_MS", 1_000_u64)?),
        max_delay: Duration::from_millis(parse_env("RETRY_MAX_DELAY_MS", 60_000_u64)?),
        scheduler_interval: Duration::from_millis(
            parse_env("RETRY_SCHEDULER_INTERVAL_MS", 1_000_u64)?,
        ),
        scheduler_batch_size: parse_env("RETRY_SCHEDULER_BATCH_SIZE", 100_usize)?,
    };

    anyhow::ensure!(
        !retry_config.base_delay.is_zero(),
        "RETRY_BASE_DELAY_MS must be greater than zero"
    );
    anyhow::ensure!(
        retry_config.max_delay >= retry_config.base_delay,
        "RETRY_MAX_DELAY_MS must be at least RETRY_BASE_DELAY_MS"
    );
    anyhow::ensure!(
        !retry_config.scheduler_interval.is_zero(),
        "RETRY_SCHEDULER_INTERVAL_MS must be greater than zero"
    );
    anyhow::ensure!(
        retry_config.scheduler_batch_size > 0,
        "RETRY_SCHEDULER_BATCH_SIZE must be greater than zero"
    );
    retry::validate_hash_tags(STREAM, &retry_config)?;

    let redis_client = redis::Client::open(redis_url).context("failed to create Redis client")?;

    let pool = PgPoolOptions::new()
        .max_connections(20)
        .min_connections(2)
        .connect(&database_url)
        .await
        .context("failed to connect to PostgreSQL")?;

    migrate(&pool)
        .await
        .context("failed to run database migrations")?;

    let consumer_name =
        env::var("CONSUMER_NAME").unwrap_or_else(|_| format!("worker-{}", Uuid::new_v4()));

    let shutdown = CancellationToken::new();
    let signal_token = shutdown.clone();

    tokio::spawn(async move {
        if let Err(error) = tokio::signal::ctrl_c().await {
            error!(error = %error, "failed to listen for Ctrl-C");
        }
        signal_token.cancel();
    });

    info!(
        consumer = %consumer_name,
        max_in_flight,
        min_idle_ms,
        recovery_interval_secs,
        fail_before_ack,
        max_retries = retry_config.max_retries,
        retry_zset = %retry_config.retry_zset,
        dead_stream = %retry_config.dead_stream,
        "redis worker starting"
    );

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        match run_worker(
            &redis_client,
            &pool,
            &consumer_name,
            max_in_flight,
            min_idle_ms,
            recovery_interval_secs,
            fail_before_ack,
            &retry_config,
            &shutdown,
        )
        .await
        {
            Ok(()) => break,
            Err(error) => {
                error!(
                    error = %error,
                    "worker stopped unexpectedly; reconnecting"
                );

                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                }
            }
        }
    }

    info!("worker shutting down");
    pool.close().await;
    Ok(())
}

fn parse_env<T>(name: &str, default: T) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("invalid value for {name}: {value}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("failed to read {name}")),
    }
}

fn is_group_already_exists(error: &redis::RedisError) -> bool {
    error.to_string().contains("BUSYGROUP")
}

async fn ensure_group(conn: &mut redis::aio::MultiplexedConnection) -> anyhow::Result<()> {
    let result: redis::RedisResult<String> =
        conn.xgroup_create_mkstream(STREAM, GROUP, "0-0").await;

    match result {
        Ok(_) => info!(stream = STREAM, group = GROUP, "consumer group created"),
        Err(error) if is_group_already_exists(&error) => {
            info!(
                stream = STREAM,
                group = GROUP,
                "consumer group already exists"
            );
        }
        Err(error) => return Err(error).context("failed to create consumer group"),
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_worker(
    client: &redis::Client,
    pool: &PgPool,
    consumer_name: &str,
    max_in_flight: usize,
    min_idle_ms: u64,
    recovery_interval_secs: u64,
    fail_before_ack: bool,
    retry_config: &RetryConfig,
    shutdown: &CancellationToken,
) -> anyhow::Result<()> {
    let mut read_conn = client
        .get_multiplexed_async_connection()
        .await
        .context("failed to open Redis read connection")?;
    let mut recovery_conn = client
        .get_multiplexed_async_connection()
        .await
        .context("failed to open Redis recovery connection")?;
    let ack_conn = client
        .get_multiplexed_async_connection()
        .await
        .context("failed to open Redis ACK connection")?;

    ensure_group(&mut read_conn).await?;

    let startup_report = recover_pending(
        &mut recovery_conn,
        pool,
        &ack_conn,
        consumer_name,
        min_idle_ms,
        max_in_flight,
        fail_before_ack,
        retry_config,
    )
    .await?;
    info!(?startup_report, "startup Pending recovery completed");

    // Recovery and retry scheduling use independent Redis connections.
    let recovery_shutdown = shutdown.child_token();
    let retry_shutdown = shutdown.child_token();
    let recovery_task = tokio::spawn({
        let pool = pool.clone();
        let ack_conn = ack_conn.clone();
        let consumer_name = consumer_name.to_owned();
        let retry_config = retry_config.clone();
        let token = recovery_shutdown.clone();

        async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(recovery_interval_secs));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = ticker.tick() => {
                        match recover_pending(
                            &mut recovery_conn,
                            &pool,
                            &ack_conn,
                            &consumer_name,
                            min_idle_ms,
                            max_in_flight,
                            fail_before_ack,
                            &retry_config,
                        ).await {
                            Ok(report) => {
                                if report.claimed > 0 || report.failed > 0 || report.deleted > 0 {
                                    info!(?report, "periodic Pending recovery completed");
                                }
                            }
                            Err(error) => {
                                error!(error = %error, "periodic Pending recovery failed");
                            }
                        }
                    }
                }
            }
        }
    });

    let retry_task = tokio::spawn(run_retry_scheduler(
        client.clone(),
        retry_config.clone(),
        retry_shutdown.clone(),
    ));

    let options = StreamReadOptions::default()
        .group(GROUP, consumer_name)
        .count(READ_BATCH_SIZE)
        .block(READ_BLOCK_MS);

    info!(
        consumer = %consumer_name,
        stream = STREAM,
        group = GROUP,
        "worker started"
    );

    let run_result: anyhow::Result<()> = 'worker: loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!(consumer = %consumer_name, "shutdown requested");
                break Ok(());
            }

            result = read_conn.xread_options(&[STREAM], &[">"], &options) => {
                let reply: Option<StreamReadReply> = match result {
                    Ok(reply) => reply,
                    Err(error) => break Err(error).context("XREADGROUP failed"),
                };

                let Some(reply) = reply else {
                    continue;
                };

                for stream in reply.keys {
                    match process_batch(
                        pool,
                        &ack_conn,
                        stream.ids,
                        max_in_flight,
                        fail_before_ack,
                        retry_config,
                    ).await {
                        Ok(report) => info!(
                            stream = %stream.key,
                            ?report,
                            "new-message batch completed"
                        ),
                        Err(error) => {
                            break 'worker Err(error).context("new-message batch failed");
                        }
                    }
                }
            }
        }
    };

    recovery_shutdown.cancel();
    retry_shutdown.cancel();

    if let Err(join_error) = recovery_task.await {
        warn!(error = %join_error, "recovery task did not exit cleanly");
    }
    if let Err(join_error) = retry_task.await {
        warn!(error = %join_error, "retry scheduler task did not exit cleanly");
    }

    run_result
}

async fn run_retry_scheduler(
    client: redis::Client,
    retry_config: RetryConfig,
    shutdown: CancellationToken,
) {
    while !shutdown.is_cancelled() {
        let mut conn = match client.get_multiplexed_async_connection().await {
            Ok(conn) => conn,
            Err(error) => {
                warn!(error = %error, "retry scheduler could not connect to Redis");
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(2)) => continue,
                }
            }
        };

        let mut ticker = tokio::time::interval(retry_config.scheduler_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;

        let mut reconnect = false;

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {}
            }

            match retry::publish_due_retries(&mut conn, STREAM, &retry_config).await {
                Ok(published) if published > 0 => {
                    info!(published, "due retry messages republished to source Stream");
                }
                Ok(_) => {}
                Err(error) => {
                    warn!(
                        error = %error,
                        "retry scheduler operation failed; reconnecting"
                    );
                    reconnect = true;
                    break;
                }
            }
        }

        if reconnect {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
        }
    }
}
