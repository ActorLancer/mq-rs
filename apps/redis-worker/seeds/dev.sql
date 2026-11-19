-- Reset the demo order so a new producer invocation can be tested.
-- Development data is intentionally kept outside versioned migrations.
INSERT INTO orders (id, status, buyer_id, currency)
VALUES (1001, 'PENDING', 2001, 'CNY')
ON CONFLICT (id) DO UPDATE
SET status = 'PENDING',
    buyer_id = EXCLUDED.buyer_id,
    currency = EXCLUDED.currency,
    payment_id = NULL,
    paid_amount_cents = NULL,
    updated_at = NOW();
