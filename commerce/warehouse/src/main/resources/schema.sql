CREATE TABLE IF NOT EXISTS warehouse_product (
    product_id UUID PRIMARY KEY,
    quantity INTEGER,
    fragile BOOLEAN,
    weight DOUBLE PRECISION,
    width DOUBLE PRECISION,
    height DOUBLE PRECISION,
    depth DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS order_bookings (
    booking_id UUID PRIMARY KEY,
    order_id UUID,
    delivery_id UUID
);

CREATE TABLE IF NOT EXISTS booking_products (
    product_id UUID PRIMARY KEY,
    booking_id UUID,
    quantity integer
);
