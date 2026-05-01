CREATE TABLE IF NOT EXISTS payments (
    payment_id UUID PRIMARY KEY,
    total_payment NUMERIC(10, 2),
    delivery_total NUMERIC(10, 2),
    fee_total NUMERIC(10, 2)
);
