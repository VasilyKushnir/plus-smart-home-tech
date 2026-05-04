CREATE TABLE IF NOT EXISTS addresses (
    address_id UUID PRIMARY KEY,
    country VARCHAR(128),
    city VARCHAR(128),
    street VARCHAR(128),
    house VARCHAR(128),
    flat VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id UUID PRIMARY KEY,
    from_adress_id UUID NOT NULL REFERENCES addresses,
    to_adress_id UUID NOT NULL REFERENCES addresses,
    order_id UUID NOT NULL,
    delivery_state VARCHAR(64) NOT NULL
);
