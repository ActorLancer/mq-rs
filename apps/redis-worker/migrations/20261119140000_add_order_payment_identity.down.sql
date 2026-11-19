ALTER TABLE orders
    DROP CONSTRAINT IF EXISTS orders_currency_format_check;

ALTER TABLE orders
    DROP COLUMN IF EXISTS buyer_id,
    DROP COLUMN IF EXISTS currency;
