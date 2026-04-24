CREATE TABLE IF NOT EXISTS products (
    product_id UUID PRIMARY KEY,
    productName VARCHAR(256) NOT NULL,
    description VARCHAR(1024) NOT NULL,
    imageSrc VARCHAR(256) NOT NULL,
    quantityState VARCHAR(64) NOT NULL,
    productState VARCHAR(64) NOT NULL,
    productCategory VARCHAR(64) NOT NULL,
    price NUMERIC(10, 2) NOT NULL
);
