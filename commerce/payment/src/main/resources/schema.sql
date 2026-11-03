CREATE TABLE IF NOT EXISTS payments (
    payment_id UUID PRIMARY KEY,
    order_id UUID,
    payment_status VARCHAR(64),
    total_payment NUMERIC(10, 2),
    delivery_total NUMERIC(10, 2),
    fee_total NUMERIC(10, 2)
);
