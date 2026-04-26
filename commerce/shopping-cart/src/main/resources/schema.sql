CREATE TABLE IF NOT EXISTS shopping_carts (
    shopping_cart_id UUID PRIMARY KEY,
    username VARCHAR(256),
    state VARCHAR(16)
);

CREATE TABLE IF NOT EXISTS shopping_cart_products (
    cart_id UUID REFERENCES shopping_carts,
    product_id UUID,
    quantity INTEGER,
    PRIMARY KEY (cart_id, product_id)
);
