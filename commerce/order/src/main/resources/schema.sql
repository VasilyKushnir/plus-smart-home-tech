CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY,
    shopping_cart_id UUID NOT NULL,
    payment_id UUID,
    delivery_id UUID,
    state VARCHAR(64),
    delivery_weight NUMERIC(10, 3),
    delivery_volume NUMERIC(10, 3),
    fragile BOOLEAN,
    total_price NUMERIC(10, 2),
    delivery_price NUMERIC(10, 2),
    product_price NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS order_products (
    order_id UUID REFERENCES orders ON DELETE CASCADE,
    product_id UUID,
    quantity INTEGER,
    PRIMARY KEY (order_id, product_id)
);
