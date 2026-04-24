CREATE TABLE IF NOT EXISTS products (
    product_id UUID PRIMARY KEY,
    product_name VARCHAR(256) NOT NULL,
    description VARCHAR(1024) NOT NULL,
    image_src VARCHAR(256) NOT NULL,
    quantity_state VARCHAR(64) NOT NULL,
    product_state VARCHAR(64) NOT NULL,
    product_category VARCHAR(64) NOT NULL,
    price NUMERIC(10, 2) NOT NULL
);
