use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// 统一事件类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// 全局唯一事件 ID
    pub event_id: Uuid,

    /// 事件类型
    pub event_type: String,

    /// 事件版本
    // 消息格式会随着业务发展而变化
    pub schema_version: u16,

    /// 事件发生时间
    pub occurred_at: DateTime<Utc>,

    /// 聚合根 ID
    // 这条事件属于哪个业务对象。例如订单创建、支付成功等事件都可以使用同一个订单 ID。
    // 它通常也用于按业务对象分组或保证同一对象的事件顺序。
    pub aggregate_id: String,

    /// 聚合根类型
    pub aggregate_type: String,

    /// 事件负载
    pub payload: Value,

    /// 链路追踪 ID，表示属于同一业务请求链路
    // 把同一个业务流程中的多条消息或事件串起来。例如下一次下单流程中，订单服务、库存服务、支付
    // 服务产生的消息，可以带相同的 `correlation_id`，方便追踪“这整件事”。
    pub correlation_id: Option<Uuid>,

    /// 引发当前事件的上游事件 ID，表示当前事件由哪个事件触发
    // 指出当前事件由哪一条上游事件直接引发。例如“订单已支付”触发“开始发货”，那么“开始发货”的
    // causation_id 可以指向“订单已支付”的 event_id。
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
            event_type: event_type.into(),
            aggregate_id: aggregate_id.into(),
            aggregate_type: aggregate_type.into(),
            payload,
            event_id: Uuid::new_v4(),
            occurred_at: Utc::now(),
            schema_version: 1,
            correlation_id: None,
            causation_id: None,
        }
    }
}
