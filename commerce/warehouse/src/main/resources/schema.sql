CREATE TABLE IF NOT EXISTS warehouse_product (
    product_id UUID PRIMARY KEY,
    quantity INTEGER,
    fragile BOOLEAN,
    weight DOUBLE PRECISION,
    width DOUBLE PRECISION,
    height DOUBLE PRECISION,
    depth DOUBLE PRECISION
);
