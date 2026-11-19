-- Add up migration script here

CREATE TABLE orders (
    id BIGINT PRIMARY KEY,

    status TEXT NOT NULL
        CHECK (status IN ('PENDING', 'PAID', 'CANCELLED')),

    payment_id UUID UNIQUE,

    paid_amount_cents BIGINT,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (
        status <> 'PAID'
        OR (
            payment_id IS NOT NULL
            AND paid_amount_cents IS NOT NULL
            AND paid_amount_cents > 0
        )
    )
);

CREATE TABLE inbox_messages (
    consumer_name TEXT NOT NULL,

    event_id UUID NOT NULL,

    event_type TEXT NOT NULL,

    -- 聚合根类型
    aggregate_type TEXT NOT NULL,

    -- 业务对象 ID
    aggregate_id TEXT NOT NULL,

    payload JSONB NOT NULL,

    -- 事件发生时间
    occurred_at TIMESTAMPTZ NOT NULL,

    -- 成功处理记录写入时间
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (consumer_name, event_id)
);

CREATE INDEX idx_inbox_processed_at
    ON inbox_messages(processed_at);

-- INSERT INTO orders (id, status)
-- VALUES (1001, 'PENDING')
-- ON CONFLICT (id) DO NOTHING;

INSERT INTO orders (id, status)
VALUES (1001, 'PENDING')
ON CONFLICT (id) DO UPDATE
SET status = 'PENDING',
    payment_id = NULL,
    paid_amount_cents = NULL;
