## **Rust 消息队列完整实战教程**

我们直接进入代码实战，不再继续做纯概念练习。

这套教程以**生产级异步消息处理系统**为目标，使用 Rust + Tokio，先实现 Redis Stream，再实现 NATS JetStream，最后将两者与 PostgreSQL Outbox、幂等消费、重试、DLQ、监控和优雅关闭整合起来。

我们不会只写一个 `XADD` 和 `XREADGROUP` 的 Demo，而是逐步构建一个可以运行、测试、注入故障、恢复消息的完整项目。

## **一、最终要构建什么？**

项目暂定名称：`rust-message-lab`。

## **最终项目架构**

**Producer / 业务服务**

生成订单事件、支付事件、通知事件

**PostgreSQL Outbox**

业务事务 + 事件记录原子提交

**Redis Stream**

Consumer Group / PEL / XACK / XAUTOCLAIM

**NATS JetStream**

Stream / Durable Consumer / ACK / Redelivery

**Rust Workers**

并发消费、超时控制、幂等处理、失败分类

**成功**

事务提交 + ACK

**临时失败**

退避重试

**永久失败**

DLQ 隔离

最终你将具备独立实现以下能力的经验：

- 异步消息生产与消费。
- 多消费者并发处理。
- 消息 ACK 与重复投递处理。
- 数据库幂等性和事务边界。
- 延迟重试、指数退避和 Jitter。
- 死信队列和人工重新投递。
- 消费者故障检测与 Pending 恢复。
- Outbox / Inbox 模式。
- 消息顺序、背压和限流。
- Redis、NATS 的持久化与故障恢复。
- Tokio 任务生命周期、CancellationToken、优雅关闭。
- 单元测试、集成测试、故障注入和性能测试。

## **二、技术栈与版本策略**

| 技术                 | 用途              |
| ------------------ | --------------- |
| Rust Stable        | 主要开发语言          |
| Tokio              | 异步运行时           |
| `redis`            | Redis 客户端       |
| `async-nats`       | NATS 客户端        |
| SQLx               | PostgreSQL 异步访问 |
| Serde              | 消息序列化           |
| UUID               | 事件 ID           |
| Chrono / time      | 时间处理            |
| thiserror          | 库和领域错误          |
| anyhow             | 应用层错误上下文        |
| tracing            | 结构化日志           |
| tracing-subscriber | 日志订阅            |
| CancellationToken  | 优雅关闭            |
| Docker Compose     | 本地基础设施          |

版本策略：使用当前 Rust Stable 和兼容的 crate 版本，实际版本由 `Cargo.lock` 固定。代码以明确的 API 为准，不混用不同版本的客户端接口。

## **三、完整实战路线**

我们分成 8 个阶段，每个阶段都要有实际代码、运行结果和测试。

| 阶段 | 内容                          | 最终成果                        |
| -- | --------------------------- | --------------------------- |
| 1  | Rust 项目工程化、基础设施、事件模型        | 可运行的 Rust 消息项目              |
| 2  | Redis Stream Producer       | 消息生产者                       |
| 3  | Redis Stream Consumer Group | 并发消费者、ACK、PEL、故障接管          |
| 4  | 消息可靠性                       | 幂等、重试、延迟队列、DLQ              |
| 5  | PostgreSQL Outbox / Inbox   | 事务消息和可靠消费                   |
| 6  | NATS JetStream              | Stream、Pull Consumer、ACK、重放 |
| 7  | 高级工程能力                      | 背压、限流、优雅关闭、监控、Tracing       |
| 8  | 综合项目                        | 完整订单事件系统、测试和性能压测            |

其中 Redis Stream 和 NATS JetStream 会分别实现一套生产者、消费者和可靠性机制。最后再比较两者的实现差异。

**开发原则：先保证正确性，再优化性能；先理解底层机制，再使用封装库。**

# **第一阶段：搭建 Rust 消息系统工程**

## **第 1 课：项目初始化和基础设施**

### **1.1 准备开发环境**

检查 Rust：

```
rustc --version
cargo --version
rustup show
```

准备 Docker：

```
docker --version
docker compose version
```

如果没有安装相关工具，Arch Linux 可以使用：

```
sudo pacman -S rustup docker docker-compose
```

根据你现有的 Arch Linux 环境，建议直接使用本机 Rust 工具链，Docker 只负责运行 Redis、NATS 和 PostgreSQL。

### **1.2 创建项目目录**

我们使用 Cargo Workspace，而不是把所有代码都堆在一个 `main.rs` 中。

```
mkdir rust-message-lab
cd rust-message-lab

mkdir -p apps/producer/src
mkdir -p apps/redis-worker/src
mkdir -p apps/nats-worker/src

mkdir -p crates/domain/src
mkdir -p crates/messaging/src
mkdir -p crates/infrastructure/src

mkdir -p tests
mkdir -p config
```

最终目录结构：

```
rust-message-lab/
│
├── Cargo.toml
├── Cargo.lock
├── .env
├── .env.example
├── .gitignore
├── docker-compose.yml
│
├── config/
│
├── apps/
│   ├── producer/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── main.rs
│   │
│   ├── redis-worker/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── main.rs
│   │
│   └── nats-worker/
│       ├── Cargo.toml
│       └── src/
│           └── main.rs
│
├── crates/
│   ├── domain/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       └── event.rs
│   │
│   ├── messaging/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── lib.rs
│   │
│   └── infrastructure/
│       ├── Cargo.toml
│       └── src/
│           └── lib.rs
│
└── tests/
```

为什么这样拆分？

| 模块             | 职责                          |
| -------------- | --------------------------- |
| domain         | 事件模型、业务类型、领域规则              |
| messaging      | 消息生产者和消费者的抽象                |
| infrastructure | Redis、NATS、PostgreSQL 的具体实现 |
| producer       | 事件生产应用                      |
| redis-worker   | Redis Stream 消费应用           |
| nats-worker    | NATS JetStream 消费应用         |

这里有一个重要的架构原则：

**领域模型不应该依赖 Redis 或 NATS。**

例如，`OrderPaidEvent` 是业务领域中的事件，不应该因为以后从 Redis 切换到 NATS，就需要修改它的定义。

这也是我们将 `domain` 独立出来的原因。

## **1.3 配置 Workspace**

根目录 `Cargo.toml`：

```

[workspace]
resolver = "3"
members = [
    "apps/producer",
    "apps/redis-worker",
    "apps/nats-worker",
    "crates/domain",
    "crates/messaging",
    "crates/infrastructure",
]

[workspace.package]
edition = "2024"
license = "MIT"
rust-version = "1.85"

[workspace.dependencies]
# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Async runtime
tokio = { version = "1", features = ["full"] }
tokio-util = { version = "0.7", features = ["rt"] }

# Messaging
redis = { version = "1", features = ["tokio-comp"] }
async-nats = "0.46"

# Database
sqlx = { version = "0.8", features = [
    "runtime-tokio",
    "tls-rustls",
    "postgres",
    "uuid",
    "chrono",
    "json",
    "migrate",
] }

# Common
uuid = { version = "1", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
thiserror = "2"
anyhow = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = [
    "env-filter",
    "fmt",
] }
dotenvy = "0.15"

```

说明：

- `tokio`：提供异步运行时。
- `redis`：Redis 异步客户端。
- `async-nats`：NATS 客户端。
- `sqlx`：PostgreSQL 数据访问。
- `serde`：事件序列化。
- `uuid`：生成唯一事件 ID。
- `chrono`：事件时间。
- `tracing`：结构化日志。
- `tokio-util`：后续使用 CancellationToken 实现优雅关闭。

这里使用较新的依赖版本约束作为起点，实际解析结果以 Cargo 的依赖解析和 `Cargo.lock` 为准。若某个 crate 的指定版本或功能发生不兼容，先以编译器和 crate 文档为准调整，不要随意混用旧版 API。

另外，`redis` 和 `async-nats` 的版本约束可能需要根据你本机 Cargo registry 中实际可用的版本调整。

### **1.4 配置 Domain Crate**

`crates/domain/Cargo.toml`：

```
[package]
name = "domain"
version = "0.1.0"
edition.workspace = true

[dependencies]
serde.workspace = true
serde_json.workspace = true
uuid.workspace = true
chrono.workspace = true
thiserror.workspace = true
```

`crates/domain/src/lib.rs`：

```
pub mod event;
```

创建 `crates/domain/src/event.rs`：

```
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// 统一事件类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// 全局唯一事件 ID
    pub event_id: Uuid,

    /// 事件类型，例如 order.paid
    pub event_type: String,

    /// 事件版本
    pub schema_version: u16,

    /// 事件发生时间
    pub occurred_at: DateTime<Utc>,

    /// 聚合根类型
    pub aggregate_type: String,

    /// 聚合根 ID
    pub aggregate_id: String,

    /// 事件负载
    pub payload: Value,

    /// 链路追踪 ID
    pub correlation_id: Option<Uuid>,

    /// 引发当前事件的上游事件 ID
    pub causation_id: Option<Uuid>,
}

impl EventEnvelope {
    pub fn new(
        event_type: impl Into<String>,
        aggregate_type: impl Into<String>,
        aggregate_id: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            event_type: event_type.into(),
            schema_version: 1,
            occurred_at: Utc::now(),
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
            payload,
            correlation_id: None,
            causation_id: None,
        }
    }
}
```

### **1.5 为什么需要统一事件结构？**

假设我们有一个订单支付成功事件：

```
{
  "event_id": "d32b6ed8-3a34-4b37-90d5-58d57a3e6e22",
  "event_type": "order.paid",
  "schema_version": 1,
  "occurred_at": "2026-11-17T14:00:00Z",
  "aggregate_type": "order",
  "aggregate_id": "1001",
  "payload": {
    "order_id": 1001,
    "buyer_id": 2001,
    "amount": 99.99,
    "currency": "CNY"
  },
  "correlation_id": null,
  "causation_id": null
}
```

这里有几个值得理解的设计。

**event_id**

每条事件拥有独立的唯一 ID。

后续消费者可以通过它实现幂等。

例如：

```
event_id = abc-123

第一次消费：
    数据库不存在 abc-123
    执行业务
    记录 abc-123 已处理

第二次消费：
    数据库已经存在 abc-123
    不重复执行
```

**event_type**

消费者根据事件类型决定如何处理：

```
match event.event_type.as_str() {
    "order.paid" => {
        // 支付成功处理
    }
    "order.cancelled" => {
        // 订单取消处理
    }
    _ => {
        // 未知事件处理
    }
}
```

**schema_version**

消息格式会随着业务发展而变化。

例如：

```
V1:
{
    "order_id": 1001,
    "amount": 99.99
}

V2:
{
    "order_id": 1001,
    "amount": 99.99,
    "currency": "CNY"
}
```

版本字段可以帮助消费者兼容不同版本的事件。

**correlation_id 和 causation_id**

这两个字段用于分布式链路追踪。

例如：

```
用户购买数据
    |
    v
order.created
    |
    v
payment.succeeded
    |
    v
order.paid
    |
    v
license.granted
```

- `correlation_id`：表示属于同一业务请求链路。
- `causation_id`：表示当前事件由哪个事件触发。

它们对后续排查异步业务故障非常有帮助。

# **四、搭建本地基础设施**

我们先通过 Docker Compose 运行三个服务。

## **2.1 Docker Compose**

根目录 `docker-compose.yml`：

```

services:
  redis:
    image: redis:7.4
    container_name: message-lab-redis
    command:
      - redis-server
      - --appendonly
      - "yes"
      - --appendfsync
      - everysec
    ports:
      - "16379:6379"
    volumes:
      - redis-data:/data
    restart: unless-stopped

  nats:
    image: nats:2.11
    container_name: message-lab-nats
    command:
      - "-js"
      - "-sd"
      - "/data"
      - "-m"
      - "8222"
    ports:
      - "14222:4222"
      - "18222:8222"
    volumes:
      - nats-data:/data
    restart: unless-stopped

  postgres:
    image: postgres:17
    container_name: message-lab-postgres
    environment:
      POSTGRES_USER: message_lab
      POSTGRES_PASSWORD: dev_password
      POSTGRES_DB: message_lab
    ports:
      - "15432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U message_lab -d message_lab"]
      interval: 5s
      timeout: 3s
      retries: 10
    restart: unless-stopped

volumes:
  redis-data:
  nats-data:
  postgres-data:

```

说明：

| 服务              | 容器端口 | 宿主机端口 |
| --------------- | ---- | ----- |
| Redis           | 6379 | 16379 |
| NATS            | 4222 | 14222 |
| NATS Monitoring | 8222 | 18222 |
| PostgreSQL      | 5432 | 15432 |

这里使用了独立端口，避免与你现有 Hermes 开发环境的 Redis、PostgreSQL 和 NATS 冲突。

注意：这里是本地开发配置，`dev_password` 是测试密码，不可以用于公网或生产部署。

启动：

```
docker compose up -d
```

查看服务：

```
docker compose ps
```

查看日志：

```
docker compose logs -f
```

## **2.2 验证 Redis**

```
redis-cli -p 16379 PING
```

预期结果：

```
PONG
```

测试 Stream：

```
XADD test:events * name alice action login
```

查看：

```
XRANGE test:events - +
```

删除测试数据：

```
DEL test:events
```

## **2.3 验证 NATS**

检查 Monitoring：

```
curl http://127.0.0.1:18222/varz
```

NATS 客户端连接地址：

```
nats://127.0.0.1:14222
```

后面我们会用 Rust 创建 JetStream Stream 和 Consumer。

## **2.4 验证 PostgreSQL**

```
psql \
  -h 127.0.0.1 \
  -p 15432 \
  -U message_lab \
  -d message_lab
```

密码：

```
dev_password
```

进入 PostgreSQL 后：

```
SELECT version();
```

# **五、配置应用环境变量**

创建 `.env.example`：

```
APP_ENV=development

REDIS_URL=redis://127.0.0.1:16379/

NATS_URL=nats://127.0.0.1:14222

DATABASE_URL=postgres://message_lab:dev_password@127.0.0.1:15432/message_lab

RUST_LOG=info
```

复制：

```
cp .env.example .env
```

`.gitignore`：

```
/target

.env

*.log

.DS_Store
```

这里不要将 `.env` 提交到 Git。

# **六、第一段真正的 Rust 消息代码：Producer**

基础设施准备好后，我们开始实现真正的消息生产者。

这一节只做一件事情：

**使用 Rust 将一个订单支付成功事件写入 Redis Stream。**

## **3.1 配置 Producer**

`apps/producer/Cargo.toml`：

```
[package]
name = "producer"
version = "0.1.0"
edition.workspace = true

[dependencies]
domain = { path = "../../crates/domain" }

tokio.workspace = true
redis.workspace = true
serde_json.workspace = true
anyhow.workspace = true
dotenvy.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
```

## **3.2 实现 Redis Producer**

`apps/producer/src/main.rs`：

```

use anyhow::Context;
use domain::event::EventEnvelope;
use redis::AsyncCommands;
use serde_json::json;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("info".parse()?)
        )
        .init();

    let redis_url = std::env::var("REDIS_URL")
        .context("REDIS_URL is not configured")?;

    let client = redis::Client::open(redis_url)
        .context("failed to create Redis client")?;

    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("failed to connect to Redis")?;

    let event = EventEnvelope::new(
        "order.paid",
        "order",
        "1001",
        json!({
            "order_id": 1001,
            "buyer_id": 2001,
            "amount": 99.99,
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

```

这里我刻意使用了 Redis 原生命令 `XADD`，而不是一开始就使用复杂的封装。

我们需要理解客户端究竟向 Redis 发送了什么。

## **3.3 逐步理解代码**

首先：

```
let client = redis::Client::open(redis_url)?;
```

创建 Redis 客户端。

注意：

创建 `Client` 并不等于已经建立了 TCP 连接。

接下来：

```
let mut conn = client
    .get_multiplexed_async_connection()
    .await?;
```

获取异步 Multiplexed Connection。

这里需要理解一个概念：

**Multiplexed Connection 可以支持多个异步任务共享一个 Redis 连接。**

但它并不代表任何情况下都适合共享。

例如：

- 普通 Redis 命令可以使用 Multiplexed Connection。
- 长时间阻塞的 `XREADGROUP BLOCK` 最好使用专门的连接。
- 对需要严格隔离的消费者任务，也可以使用独立连接。

然后：

```
let event = EventEnvelope::new(
    "order.paid",
    "order",
    "1001",
    json!({
        "order_id": 1001,
        "buyer_id": 2001,
        "amount": 99.99,
        "currency": "CNY"
    }),
);
```

创建业务事件。

这里 `event_id` 在创建时就生成了。

后续即使 Redis 发布失败并重试，必须保留同一个事件 ID，而不能每次重试都创建一个新的事件 ID。

否则消费者无法识别这是同一条业务事件。

最后：

```
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
    .await?;
```

对应 Redis 命令：

```
XADD orders:events * event_id abc-123 event_type order.paid payload "{...}"
```

其中：

- `orders:events`：Stream 名称。
- `*`：让 Redis 自动生成消息 ID。
- `event_id`：我们业务系统自己的事件 ID。
- `event_type`：事件类型。
- `payload`：完整事件 JSON。

特别注意：

**Redis Stream ID 和业务 event_id 不是同一个概念。**

| 字段              | 示例                | 用途           |
| --------------- | ----------------- | ------------ |
| Redis Stream ID | `1794900000000-0` | Redis 内部消息定位 |
| event_id        | UUID              | 业务幂等和跨系统追踪   |

这个区分非常重要。

# **七、运行第一个 Rust Producer**

先检查整个 Workspace：

```
cargo check --workspace
```

如果暂时还没有创建其他 crate 的 `Cargo.toml` 和 `lib.rs`，Cargo 会提示找不到成员。此时可以先只创建本课已经涉及的成员，或者先将尚未实现的成员暂时从 `members` 中移除。

运行 Producer：

```
cargo run -p producer
```

预期日志类似：

```
INFO event published
event_id=...
stream_id=...
event_type=order.paid
```

然后检查 Redis：

```
redis-cli -p 16379
```

执行：

```
XRANGE orders:events - +
```

你应该可以看到刚刚由 Rust 发布的订单事件。

第一个实战目标

Rust → Tokio → redis-rs → Redis Stream → XADD

# **八、这一版代码距离生产级还有什么差距？**

虽然已经成功发布消息，但目前还不能称为可靠消息系统。

我们主动保留了几个问题，后续逐步解决。

| 当前问题              | 后续解决方案                          |
| ----------------- | ------------------------------- |
| Redis 不可用时直接报错    | 连接重试、退避和熔断策略                    |
| 数据库提交后 Redis 发布失败 | PostgreSQL Transactional Outbox |
| 不知道消息是否被消费者处理     | Consumer Group + ACK            |
| 消费者处理后崩溃          | PEL + XAUTOCLAIM                |
| 消息重复投递            | PostgreSQL Inbox + 唯一约束         |
| 消息处理失败            | 重试策略 + DLQ                      |
| 消息量过大             | 批量写入、背压和并发控制                    |
| 程序关闭时任务中断         | CancellationToken + 优雅关闭        |

另外还有一个非常重要的问题：

目前的 `XADD` 只是一次 Redis 写入操作。如果 Redis 返回成功，代表它接受了这次写入，但不能仅凭这个结果就认为消息已经可靠地完成了跨系统持久化和业务处理。

后面会逐步处理这些边界。

# **九、本课作业与验收**

这次不安排大量理论题，而是让你真正运行代码。

**第一课验收清单**

0/8 已完成

创建 rust-message-lab Cargo Workspace

启动 Redis、NATS、PostgreSQL 三个 Docker 服务

创建 EventEnvelope 事件结构

完成 Redis Producer 代码

成功使用 cargo check 编译

成功运行 cargo run -p producer

使用 XRANGE 查看 Rust 发布的消息

解释 Redis Stream ID 和 event_id 的区别

**需要特别回答的三个问题：**

1. 为什么 `EventEnvelope` 中的 `event_id` 不能在每次发布重试时重新生成？
2. 为什么 Redis Stream ID 和业务 `event_id` 应该分开？
3. 假设 PostgreSQL 已经提交订单支付成功，但是 Redis 的 `XADD` 失败了，目前这版 Producer 能否保证事件不丢失？为什么？

完成后，把你的项目目录结构、`Cargo.toml`、Producer 代码和运行结果发给我。我会按照代码审查的方式检查，包括错误处理、异步连接使用、事件结构设计和可扩展性。

**下一课：Redis Stream Consumer Group 实战。** 我们会用 Rust 实现 `XGROUP CREATE`、`XREADGROUP`、批量消息处理、并发控制和 `XACK`，并且亲自模拟 Worker 崩溃，观察 PEL 和消息接管过程。

这套架构也会尽量保持与你的 Hermes 项目现有 Rust、PostgreSQL、Redis 和 NATS 技术栈一致。 **Rust 消息队列完整实战教程**

我们直接进入代码实战，不再继续做纯概念练习。

这套教程以**生产级异步消息处理系统**为目标，使用 Rust + Tokio，先实现 Redis Stream，再实现 NATS JetStream，最后将两者与 PostgreSQL Outbox、幂等消费、重试、DLQ、监控和优雅关闭整合起来。

我们不会只写一个 `XADD` 和 `XREADGROUP` 的 Demo，而是逐步构建一个可以运行、测试、注入故障、恢复消息的完整项目。

## **一、最终要构建什么？**

项目暂定名称：`rust-message-lab`。

## **最终项目架构**

**Producer / 业务服务**

生成订单事件、支付事件、通知事件

**PostgreSQL Outbox**

业务事务 + 事件记录原子提交

**Redis Stream**

Consumer Group / PEL / XACK / XAUTOCLAIM

**NATS JetStream**

Stream / Durable Consumer / ACK / Redelivery

**Rust Workers**

并发消费、超时控制、幂等处理、失败分类

**成功**

事务提交 + ACK

**临时失败**

退避重试

**永久失败**

DLQ 隔离

最终你将具备独立实现以下能力的经验：

- 异步消息生产与消费。
- 多消费者并发处理。
- 消息 ACK 与重复投递处理。
- 数据库幂等性和事务边界。
- 延迟重试、指数退避和 Jitter。
- 死信队列和人工重新投递。
- 消费者故障检测与 Pending 恢复。
- Outbox / Inbox 模式。
- 消息顺序、背压和限流。
- Redis、NATS 的持久化与故障恢复。
- Tokio 任务生命周期、CancellationToken、优雅关闭。
- 单元测试、集成测试、故障注入和性能测试。

## **二、技术栈与版本策略**

| 技术                 | 用途              |
| ------------------ | --------------- |
| Rust Stable        | 主要开发语言          |
| Tokio              | 异步运行时           |
| `redis`            | Redis 客户端       |
| `async-nats`       | NATS 客户端        |
| SQLx               | PostgreSQL 异步访问 |
| Serde              | 消息序列化           |
| UUID               | 事件 ID           |
| Chrono / time      | 时间处理            |
| thiserror          | 库和领域错误          |
| anyhow             | 应用层错误上下文        |
| tracing            | 结构化日志           |
| tracing-subscriber | 日志订阅            |
| CancellationToken  | 优雅关闭            |
| Docker Compose     | 本地基础设施          |

版本策略：使用当前 Rust Stable 和兼容的 crate 版本，实际版本由 `Cargo.lock` 固定。代码以明确的 API 为准，不混用不同版本的客户端接口。

## **三、完整实战路线**

我们分成 8 个阶段，每个阶段都要有实际代码、运行结果和测试。

| 阶段 | 内容                          | 最终成果                        |
| -- | --------------------------- | --------------------------- |
| 1  | Rust 项目工程化、基础设施、事件模型        | 可运行的 Rust 消息项目              |
| 2  | Redis Stream Producer       | 消息生产者                       |
| 3  | Redis Stream Consumer Group | 并发消费者、ACK、PEL、故障接管          |
| 4  | 消息可靠性                       | 幂等、重试、延迟队列、DLQ              |
| 5  | PostgreSQL Outbox / Inbox   | 事务消息和可靠消费                   |
| 6  | NATS JetStream              | Stream、Pull Consumer、ACK、重放 |
| 7  | 高级工程能力                      | 背压、限流、优雅关闭、监控、Tracing       |
| 8  | 综合项目                        | 完整订单事件系统、测试和性能压测            |

其中 Redis Stream 和 NATS JetStream 会分别实现一套生产者、消费者和可靠性机制。最后再比较两者的实现差异。

**开发原则：先保证正确性，再优化性能；先理解底层机制，再使用封装库。**

# **第一阶段：搭建 Rust 消息系统工程**

## **第 1 课：项目初始化和基础设施**

### **1.1 准备开发环境**

检查 Rust：

```
rustc --version
cargo --version
rustup show
```

准备 Docker：

```
docker --version
docker compose version
```

如果没有安装相关工具，Arch Linux 可以使用：

```
sudo pacman -S rustup docker docker-compose
```

根据你现有的 Arch Linux 环境，建议直接使用本机 Rust 工具链，Docker 只负责运行 Redis、NATS 和 PostgreSQL。

### **1.2 创建项目目录**

我们使用 Cargo Workspace，而不是把所有代码都堆在一个 `main.rs` 中。

```
mkdir rust-message-lab
cd rust-message-lab

mkdir -p apps/producer/src
mkdir -p apps/redis-worker/src
mkdir -p apps/nats-worker/src

mkdir -p crates/domain/src
mkdir -p crates/messaging/src
mkdir -p crates/infrastructure/src

mkdir -p tests
mkdir -p config
```

最终目录结构：

```
rust-message-lab/
│
├── Cargo.toml
├── Cargo.lock
├── .env
├── .env.example
├── .gitignore
├── docker-compose.yml
│
├── config/
│
├── apps/
│   ├── producer/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── main.rs
│   │
│   ├── redis-worker/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── main.rs
│   │
│   └── nats-worker/
│       ├── Cargo.toml
│       └── src/
│           └── main.rs
│
├── crates/
│   ├── domain/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       └── event.rs
│   │
│   ├── messaging/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── lib.rs
│   │
│   └── infrastructure/
│       ├── Cargo.toml
│       └── src/
│           └── lib.rs
│
└── tests/
```

为什么这样拆分？

| 模块             | 职责                          |
| -------------- | --------------------------- |
| domain         | 事件模型、业务类型、领域规则              |
| messaging      | 消息生产者和消费者的抽象                |
| infrastructure | Redis、NATS、PostgreSQL 的具体实现 |
| producer       | 事件生产应用                      |
| redis-worker   | Redis Stream 消费应用           |
| nats-worker    | NATS JetStream 消费应用         |

这里有一个重要的架构原则：

**领域模型不应该依赖 Redis 或 NATS。**

例如，`OrderPaidEvent` 是业务领域中的事件，不应该因为以后从 Redis 切换到 NATS，就需要修改它的定义。

这也是我们将 `domain` 独立出来的原因。

## **1.3 配置 Workspace**

根目录 `Cargo.toml`：

```

[workspace]
resolver = "3"
members = [
    "apps/producer",
    "apps/redis-worker",
    "apps/nats-worker",
    "crates/domain",
    "crates/messaging",
    "crates/infrastructure",
]

[workspace.package]
edition = "2024"
license = "MIT"
rust-version = "1.85"

[workspace.dependencies]
# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Async runtime
tokio = { version = "1", features = ["full"] }
tokio-util = { version = "0.7", features = ["rt"] }

# Messaging
redis = { version = "1", features = ["tokio-comp"] }
async-nats = "0.46"

# Database
sqlx = { version = "0.8", features = [
    "runtime-tokio",
    "tls-rustls",
    "postgres",
    "uuid",
    "chrono",
    "json",
    "migrate",
] }

# Common
uuid = { version = "1", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
thiserror = "2"
anyhow = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = [
    "env-filter",
    "fmt",
] }
dotenvy = "0.15"

```

说明：

- `tokio`：提供异步运行时。
- `redis`：Redis 异步客户端。
- `async-nats`：NATS 客户端。
- `sqlx`：PostgreSQL 数据访问。
- `serde`：事件序列化。
- `uuid`：生成唯一事件 ID。
- `chrono`：事件时间。
- `tracing`：结构化日志。
- `tokio-util`：后续使用 CancellationToken 实现优雅关闭。

这里使用较新的依赖版本约束作为起点，实际解析结果以 Cargo 的依赖解析和 `Cargo.lock` 为准。若某个 crate 的指定版本或功能发生不兼容，先以编译器和 crate 文档为准调整，不要随意混用旧版 API。

另外，`redis` 和 `async-nats` 的版本约束可能需要根据你本机 Cargo registry 中实际可用的版本调整。

### **1.4 配置 Domain Crate**

`crates/domain/Cargo.toml`：

```
[package]
name = "domain"
version = "0.1.0"
edition.workspace = true

[dependencies]
serde.workspace = true
serde_json.workspace = true
uuid.workspace = true
chrono.workspace = true
thiserror.workspace = true
```

`crates/domain/src/lib.rs`：

```
pub mod event;
```

创建 `crates/domain/src/event.rs`：

```
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// 统一事件类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// 全局唯一事件 ID
    pub event_id: Uuid,

    /// 事件类型，例如 order.paid
    pub event_type: String,

    /// 事件版本
    pub schema_version: u16,

    /// 事件发生时间
    pub occurred_at: DateTime<Utc>,

    /// 聚合根类型
    pub aggregate_type: String,

    /// 聚合根 ID
    pub aggregate_id: String,

    /// 事件负载
    pub payload: Value,

    /// 链路追踪 ID
    pub correlation_id: Option<Uuid>,

    /// 引发当前事件的上游事件 ID
    pub causation_id: Option<Uuid>,
}

impl EventEnvelope {
    pub fn new(
        event_type: impl Into<String>,
        aggregate_type: impl Into<String>,
        aggregate_id: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            event_type: event_type.into(),
            schema_version: 1,
            occurred_at: Utc::now(),
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
            payload,
            correlation_id: None,
            causation_id: None,
        }
    }
}
```

### **1.5 为什么需要统一事件结构？**

假设我们有一个订单支付成功事件：

```
{
  "event_id": "d32b6ed8-3a34-4b37-90d5-58d57a3e6e22",
  "event_type": "order.paid",
  "schema_version": 1,
  "occurred_at": "2026-11-17T14:00:00Z",
  "aggregate_type": "order",
  "aggregate_id": "1001",
  "payload": {
    "order_id": 1001,
    "buyer_id": 2001,
    "amount": 99.99,
    "currency": "CNY"
  },
  "correlation_id": null,
  "causation_id": null
}
```

这里有几个值得理解的设计。

**event_id**

每条事件拥有独立的唯一 ID。

后续消费者可以通过它实现幂等。

例如：

```
event_id = abc-123

第一次消费：
    数据库不存在 abc-123
    执行业务
    记录 abc-123 已处理

第二次消费：
    数据库已经存在 abc-123
    不重复执行
```

**event_type**

消费者根据事件类型决定如何处理：

```
match event.event_type.as_str() {
    "order.paid" => {
        // 支付成功处理
    }
    "order.cancelled" => {
        // 订单取消处理
    }
    _ => {
        // 未知事件处理
    }
}
```

**schema_version**

消息格式会随着业务发展而变化。

例如：

```
V1:
{
    "order_id": 1001,
    "amount": 99.99
}

V2:
{
    "order_id": 1001,
    "amount": 99.99,
    "currency": "CNY"
}
```

版本字段可以帮助消费者兼容不同版本的事件。

**correlation_id 和 causation_id**

这两个字段用于分布式链路追踪。

例如：

```
用户购买数据
    |
    v
order.created
    |
    v
payment.succeeded
    |
    v
order.paid
    |
    v
license.granted
```

- `correlation_id`：表示属于同一业务请求链路。
- `causation_id`：表示当前事件由哪个事件触发。

它们对后续排查异步业务故障非常有帮助。

# **四、搭建本地基础设施**

我们先通过 Docker Compose 运行三个服务。

## **2.1 Docker Compose**

根目录 `docker-compose.yml`：

```

services:
  redis:
    image: redis:7.4
    container_name: message-lab-redis
    command:
      - redis-server
      - --appendonly
      - "yes"
      - --appendfsync
      - everysec
    ports:
      - "16379:6379"
    volumes:
      - redis-data:/data
    restart: unless-stopped

  nats:
    image: nats:2.11
    container_name: message-lab-nats
    command:
      - "-js"
      - "-sd"
      - "/data"
      - "-m"
      - "8222"
    ports:
      - "14222:4222"
      - "18222:8222"
    volumes:
      - nats-data:/data
    restart: unless-stopped

  postgres:
    image: postgres:17
    container_name: message-lab-postgres
    environment:
      POSTGRES_USER: message_lab
      POSTGRES_PASSWORD: dev_password
      POSTGRES_DB: message_lab
    ports:
      - "15432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U message_lab -d message_lab"]
      interval: 5s
      timeout: 3s
      retries: 10
    restart: unless-stopped

volumes:
  redis-data:
  nats-data:
  postgres-data:

```

说明：

| 服务              | 容器端口 | 宿主机端口 |
| --------------- | ---- | ----- |
| Redis           | 6379 | 16379 |
| NATS            | 4222 | 14222 |
| NATS Monitoring | 8222 | 18222 |
| PostgreSQL      | 5432 | 15432 |

这里使用了独立端口，避免与你现有 Hermes 开发环境的 Redis、PostgreSQL 和 NATS 冲突。

注意：这里是本地开发配置，`dev_password` 是测试密码，不可以用于公网或生产部署。

启动：

```
docker compose up -d
```

查看服务：

```
docker compose ps
```

查看日志：

```
docker compose logs -f
```

## **2.2 验证 Redis**

```
redis-cli -p 16379 PING
```

预期结果：

```
PONG
```

测试 Stream：

```
XADD test:events * name alice action login
```

查看：

```
XRANGE test:events - +
```

删除测试数据：

```
DEL test:events
```

## **2.3 验证 NATS**

检查 Monitoring：

```
curl http://127.0.0.1:18222/varz
```

NATS 客户端连接地址：

```
nats://127.0.0.1:14222
```

后面我们会用 Rust 创建 JetStream Stream 和 Consumer。

## **2.4 验证 PostgreSQL**

```
psql \
  -h 127.0.0.1 \
  -p 15432 \
  -U message_lab \
  -d message_lab
```

密码：

```
dev_password
```

进入 PostgreSQL 后：

```
SELECT version();
```

# **五、配置应用环境变量**

创建 `.env.example`：

```
APP_ENV=development

REDIS_URL=redis://127.0.0.1:16379/

NATS_URL=nats://127.0.0.1:14222

DATABASE_URL=postgres://message_lab:dev_password@127.0.0.1:15432/message_lab

RUST_LOG=info
```

复制：

```
cp .env.example .env
```

`.gitignore`：

```
/target

.env

*.log

.DS_Store
```

这里不要将 `.env` 提交到 Git。

# **六、第一段真正的 Rust 消息代码：Producer**

基础设施准备好后，我们开始实现真正的消息生产者。

这一节只做一件事情：

**使用 Rust 将一个订单支付成功事件写入 Redis Stream。**

## **3.1 配置 Producer**

`apps/producer/Cargo.toml`：

```
[package]
name = "producer"
version = "0.1.0"
edition.workspace = true

[dependencies]
domain = { path = "../../crates/domain" }

tokio.workspace = true
redis.workspace = true
serde_json.workspace = true
anyhow.workspace = true
dotenvy.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
```

## **3.2 实现 Redis Producer**

`apps/producer/src/main.rs`：

```

use anyhow::Context;
use domain::event::EventEnvelope;
use redis::AsyncCommands;
use serde_json::json;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("info".parse()?)
        )
        .init();

    let redis_url = std::env::var("REDIS_URL")
        .context("REDIS_URL is not configured")?;

    let client = redis::Client::open(redis_url)
        .context("failed to create Redis client")?;

    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("failed to connect to Redis")?;

    let event = EventEnvelope::new(
        "order.paid",
        "order",
        "1001",
        json!({
            "order_id": 1001,
            "buyer_id": 2001,
            "amount": 99.99,
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

```

这里我刻意使用了 Redis 原生命令 `XADD`，而不是一开始就使用复杂的封装。

我们需要理解客户端究竟向 Redis 发送了什么。

## **3.3 逐步理解代码**

首先：

```
let client = redis::Client::open(redis_url)?;
```

创建 Redis 客户端。

注意：

创建 `Client` 并不等于已经建立了 TCP 连接。

接下来：

```
let mut conn = client
    .get_multiplexed_async_connection()
    .await?;
```

获取异步 Multiplexed Connection。

这里需要理解一个概念：

**Multiplexed Connection 可以支持多个异步任务共享一个 Redis 连接。**

但它并不代表任何情况下都适合共享。

例如：

- 普通 Redis 命令可以使用 Multiplexed Connection。
- 长时间阻塞的 `XREADGROUP BLOCK` 最好使用专门的连接。
- 对需要严格隔离的消费者任务，也可以使用独立连接。

然后：

```
let event = EventEnvelope::new(
    "order.paid",
    "order",
    "1001",
    json!({
        "order_id": 1001,
        "buyer_id": 2001,
        "amount": 99.99,
        "currency": "CNY"
    }),
);
```

创建业务事件。

这里 `event_id` 在创建时就生成了。

后续即使 Redis 发布失败并重试，必须保留同一个事件 ID，而不能每次重试都创建一个新的事件 ID。

否则消费者无法识别这是同一条业务事件。

最后：

```
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
    .await?;
```

对应 Redis 命令：

```
XADD orders:events * event_id abc-123 event_type order.paid payload "{...}"
```

其中：

- `orders:events`：Stream 名称。
- `*`：让 Redis 自动生成消息 ID。
- `event_id`：我们业务系统自己的事件 ID。
- `event_type`：事件类型。
- `payload`：完整事件 JSON。

特别注意：

**Redis Stream ID 和业务 event_id 不是同一个概念。**

| 字段              | 示例                | 用途           |
| --------------- | ----------------- | ------------ |
| Redis Stream ID | `1794900000000-0` | Redis 内部消息定位 |
| event_id        | UUID              | 业务幂等和跨系统追踪   |

这个区分非常重要。

# **七、运行第一个 Rust Producer**

先检查整个 Workspace：

```
cargo check --workspace
```

如果暂时还没有创建其他 crate 的 `Cargo.toml` 和 `lib.rs`，Cargo 会提示找不到成员。此时可以先只创建本课已经涉及的成员，或者先将尚未实现的成员暂时从 `members` 中移除。

运行 Producer：

```
cargo run -p producer
```

预期日志类似：

```
INFO event published
event_id=...
stream_id=...
event_type=order.paid
```

然后检查 Redis：

```
redis-cli -p 16379
```

执行：

```
XRANGE orders:events - +
```

你应该可以看到刚刚由 Rust 发布的订单事件。

第一个实战目标

Rust → Tokio → redis-rs → Redis Stream → XADD

# **八、这一版代码距离生产级还有什么差距？**

虽然已经成功发布消息，但目前还不能称为可靠消息系统。

我们主动保留了几个问题，后续逐步解决。

| 当前问题              | 后续解决方案                          |
| ----------------- | ------------------------------- |
| Redis 不可用时直接报错    | 连接重试、退避和熔断策略                    |
| 数据库提交后 Redis 发布失败 | PostgreSQL Transactional Outbox |
| 不知道消息是否被消费者处理     | Consumer Group + ACK            |
| 消费者处理后崩溃          | PEL + XAUTOCLAIM                |
| 消息重复投递            | PostgreSQL Inbox + 唯一约束         |
| 消息处理失败            | 重试策略 + DLQ                      |
| 消息量过大             | 批量写入、背压和并发控制                    |
| 程序关闭时任务中断         | CancellationToken + 优雅关闭        |

另外还有一个非常重要的问题：

目前的 `XADD` 只是一次 Redis 写入操作。如果 Redis 返回成功，代表它接受了这次写入，但不能仅凭这个结果就认为消息已经可靠地完成了跨系统持久化和业务处理。

后面会逐步处理这些边界。

# **九、本课作业与验收**

这次不安排大量理论题，而是让你真正运行代码。

**第一课验收清单**

0/8 已完成

创建 rust-message-lab Cargo Workspace

启动 Redis、NATS、PostgreSQL 三个 Docker 服务

创建 EventEnvelope 事件结构

完成 Redis Producer 代码

成功使用 cargo check 编译

成功运行 cargo run -p producer

使用 XRANGE 查看 Rust 发布的消息

解释 Redis Stream ID 和 event_id 的区别

**需要特别回答的三个问题：**

1. 为什么 `EventEnvelope` 中的 `event_id` 不能在每次发布重试时重新生成？
2. 为什么 Redis Stream ID 和业务 `event_id` 应该分开？
3. 假设 PostgreSQL 已经提交订单支付成功，但是 Redis 的 `XADD` 失败了，目前这版 Producer 能否保证事件不丢失？为什么？

完成后，把你的项目目录结构、`Cargo.toml`、Producer 代码和运行结果发给我。我会按照代码审查的方式检查，包括错误处理、异步连接使用、事件结构设计和可扩展性。

**下一课：Redis Stream Consumer Group 实战。** 我们会用 Rust 实现 `XGROUP CREATE`、`XREADGROUP`、批量消息处理、并发控制和 `XACK`，并且亲自模拟 Worker 崩溃，观察 PEL 和消息接管过程。

这套架构也会尽量保持与你的 Hermes 项目现有 Rust、PostgreSQL、Redis 和 NATS 技术栈一致。
