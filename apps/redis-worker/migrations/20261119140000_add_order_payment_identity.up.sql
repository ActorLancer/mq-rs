-- Backfill the current development fixture with its buyer and currency.
ALTER TABLE orders
    ADD COLUMN buyer_id BIGINT NOT NULL DEFAULT 2001,
    ADD COLUMN currency TEXT NOT NULL DEFAULT 'CNY';

ALTER TABLE orders
    ADD CONSTRAINT orders_currency_format_check
    CHECK (currency ~ '^[A-Z]{3}$');

ALTER TABLE orders
    ALTER COLUMN buyer_id DROP DEFAULT,
    ALTER COLUMN currency DROP DEFAULT;
