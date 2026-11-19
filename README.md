# mq-rs

A Rust learning project for reliable asynchronous messaging. The current executable path is Redis Streams + PostgreSQL Inbox; NATS JetStream will be added when that stage is implemented rather than kept as an empty workspace placeholder.

## Current workspace

- domain: transport-independent event envelope.
- producer: publishes an order.paid event to Redis Streams.
- redis-worker: reads Consumer Group messages, applies a PostgreSQL Inbox transaction, ACKs after commit, and periodically recovers stale Pending entries with XAUTOCLAIM.

Only implemented crates are workspace members. Empty messaging/infrastructure crates and an empty NATS executable are not kept in the active workspace.

## Run locally

```bash
cp .env.example .env
docker compose up -d
cargo check --workspace --all-targets
```

Run database migrations by starting the worker once:

```bash
cargo run -p redis-worker
```

In another terminal, reset the development order fixture and publish an event:

```bash
psql -h 127.0.0.1 -p 65432 -U message_lab -d message_lab \
  -f apps/redis-worker/seeds/dev.sql
cargo run -p producer
```

The worker should process the event, commit the Inbox + order update in one PostgreSQL transaction, then issue XACK.

## Fault injection

To reproduce the database-commit / Redis-ACK failure window:

```bash
FAIL_BEFORE_ACK=true MIN_IDLE_MS=10000 cargo run -p redis-worker
```

The worker commits PostgreSQL, deliberately skips XACK, and leaves the Redis entry Pending. A recovery scan can later claim it. The Inbox unique key (consumer_name, event_id) makes the replay a duplicate, so the business update is not applied twice.

Reset the order before publishing a new payment event:

```bash
psql -h 127.0.0.1 -p 65432 -U message_lab -d message_lab \
  -f apps/redis-worker/seeds/dev.sql
```

## Important boundaries

- Redis Stream ID identifies a transport entry; event_id identifies the business event.
- Inbox and database side effects share one PostgreSQL transaction.
- Redis XACK happens only after that transaction commits.
- XAUTOCLAIM changes PEL ownership; it does not stop an old worker task. Database idempotency is still required.
- This stage handles database side effects. External side effects must be emitted through an Outbox, not executed inside the Inbox transaction.
