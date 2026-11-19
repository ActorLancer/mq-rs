# mq-rs

A Rust learning project for reliable asynchronous messaging. The currently integrated executable path is Redis Streams + PostgreSQL Inbox. NATS JetStream will be introduced in its own stage, rather than keeping an empty NATS worker or unused infrastructure crates in the active workspace.

## Current workspace

- `domain`: transport-independent event envelope.
- `producer`: publishes an `order.paid` event to Redis Streams.
- `redis-worker`: reads Consumer Group messages, applies a PostgreSQL Inbox transaction, ACKs after commit, recovers stale Pending entries with `XAUTOCLAIM`, schedules transient failures in a ZSET, and transfers permanent/exhausted failures into a DLQ Stream.

The Redis worker modules are exposed through its library crate and used by the binary and integration tests. Empty messaging/infrastructure crates and the empty NATS executable are not kept in the active workspace.

## Run locally

```bash
cp .env.example .env
docker compose up -d
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

The integration tests use `DATABASE_URL` and run the embedded migrations. They create isolated order IDs and logical consumer names, then clean up their rows. Use a disposable development/test database.

Run database migrations and start the worker:

```bash
cargo run -p redis-worker
```

In another terminal, reset the development order fixture and publish an event:

```bash
psql -h 127.0.0.1 -p 65432 -U message_lab -d message_lab \
  -f apps/redis-worker/seeds/dev.sql
cargo run -p producer
```

The worker processes the event, commits the Inbox + order update in one PostgreSQL transaction, and then issues `XACK`. The source Stream is `orders:{events}:main`; its retry schedule and DLQ use the same `{events}` Redis Cluster hash tag.

## Fault injection

To reproduce the database-commit / Redis-ACK failure window:

```bash
FAIL_BEFORE_ACK=true MIN_IDLE_MS=10000 cargo run -p redis-worker
```

The worker commits PostgreSQL, deliberately skips `XACK`, and leaves the Redis entry Pending. A recovery scan can later claim it. The Inbox unique key `(consumer_name, event_id)` makes the replay a duplicate, so the business update is not applied twice.

Reset the order before publishing a new payment event:

```bash
psql -h 127.0.0.1 -p 65432 -U message_lab -d message_lab \
  -f apps/redis-worker/seeds/dev.sql
```

## Retry and DLQ

The worker classifies a failed delivery:

- selected SQLx connection/pool errors and transient SQLSTATEs are retryable;
- payload validation and business-state errors are permanent by default;
- retryable failures receive exponential backoff with jitter and a maximum retry count;
- permanent failures and exhausted retries are transferred into `orders:{events}:dead`.

The retry scheduler reads due members from `orders:{events}:retry` and republishes them into `orders:{events}:main`. Lua scripts perform the source PEL transition, retry scheduling, DLQ transfer, and due retry publication atomically with respect to other Redis commands. Since Redis Lua scripts do not roll back prior writes after a runtime error, scripts preflight key types and the recommended production memory policy is `noeviction`.

Inspect the retry schedule and DLQ:

```bash
redis-cli -p 16379 ZRANGE 'orders:{events}:retry' 0 -1 WITHSCORES
redis-cli -p 16379 XRANGE 'orders:{events}:dead' - +
```

The retry count is an application-level counter stored in the message. It is not the same as Redis' PEL delivery count.

## Important boundaries

- Redis Stream ID identifies a transport entry; `event_id` identifies the business event.
- Inbox and database side effects share one PostgreSQL transaction.
- Redis `XACK` happens only after that transaction commits.
- `XAUTOCLAIM` changes PEL ownership; it does not stop an old worker task. Database idempotency is still required.
- This stage handles database side effects. External side effects must be emitted through an Outbox, not executed inside the Inbox transaction.
