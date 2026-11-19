mod inbox;
mod processor;
mod recovery;

use anyhow::Context;
use redis::{
    AsyncCommands,
    streams::{StreamReadOptions, StreamReadReply},
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{env, time::Duration};
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    processor::{process_batch, STREAM, GROUP},
    recovery::recover_pending,
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
    let database_url =
        env::var("DATABASE_URL").context("DATABASE_URL is not configured")?;

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

    let redis_client =
        redis::Client::open(redis_url).context("failed to create Redis client")?;

    let pool = PgPoolOptions::new()
        .max_connections(20)
        .min_connections(2)
        .connect(&database_url)
        .await
        .context("failed to connect to PostgreSQL")?;

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .context("failed to run database migrations")?;

    let consumer_name = env::var("CONSUMER_NAME")
        .unwrap_or_else(|_| format!("worker-{}", Uuid::new_v4()));

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

async fn ensure_group(
    conn: &mut redis::aio::MultiplexedConnection,
) -> anyhow::Result<()> {
    let result: redis::RedisResult<String> =
        conn.xgroup_create_mkstream(STREAM, GROUP, "0-0").await;

    match result {
        Ok(_) => info!(stream = STREAM, group = GROUP, "consumer group created"),
        Err(error) if is_group_already_exists(&error) => {
            info!(stream = STREAM, group = GROUP, "consumer group already exists");
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
    shutdown: &CancellationToken,
) -> anyhow::Result<()> {
    // BLOCK reads get a dedicated connection. Recovery and ACK commands use
    // separate multiplexed connections so a blocked read never stalls them.
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

    // Recover old Pending messages once on startup, then periodically.
    let startup_report = recover_pending(
        &mut recovery_conn,
        pool,
        &ack_conn,
        consumer_name,
        min_idle_ms,
        max_in_flight,
        fail_before_ack,
    )
    .await?;
    info!(?startup_report, "startup pending recovery completed");

    let options = StreamReadOptions::default()
        .group(GROUP, consumer_name)
        .count(READ_BATCH_SIZE)
        .block(READ_BLOCK_MS);

    let mut recovery_tick = interval(Duration::from_secs(recovery_interval_secs));
    recovery_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // The first interval tick is immediate; consume it because startup recovery
    // has already been performed above.
    recovery_tick.tick().await;

    info!(
        consumer = %consumer_name,
        stream = STREAM,
        group = GROUP,
        "worker started"
    );

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!(consumer = %consumer_name, "shutdown requested");
                break;
            }

            _ = recovery_tick.tick() => {
                let report = recover_pending(
                    &mut recovery_conn,
                    pool,
                    &ack_conn,
                    consumer_name,
                    min_idle_ms,
                    max_in_flight,
                    fail_before_ack,
                )
                .await?;

                if report.claimed > 0 || report.failed > 0 || report.deleted > 0 {
                    info!(?report, "periodic pending recovery completed");
                }
            }

            result = read_conn.xread_options(&[STREAM], &[">"], &options) => {
                let reply: Option<StreamReadReply> =
                    result.context("XREADGROUP failed")?;

                let Some(reply) = reply else {
                    continue;
                };

                for stream in reply.keys {
                    let report = process_batch(
                        pool,
                        &ack_conn,
                        stream.ids,
                        max_in_flight,
                        fail_before_ack,
                    )
                    .await?;

                    info!(
                        stream = %stream.key,
                        ?report,
                        "new-message batch completed"
                    );
                }
            }
        }
    }

    Ok(())
}
