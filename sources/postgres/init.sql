-- Izvor 1: operativna (OLTP) baza online prodavnice tehnike.
-- Podatke puni generator (generator/generate.py) preko COPY naredbi u sources/files/postgres/batch_XX/load.sql.
-- Svaka tabela ima updated_at: ingestion u lakehouse cita samo redove novije od poslednjeg ucitanog (watermark).

CREATE TABLE customers (
    customer_id    INT PRIMARY KEY,
    first_name     TEXT        NOT NULL,
    last_name      TEXT        NOT NULL,
    email          TEXT        NOT NULL,
    country        CHAR(2)     NOT NULL,
    city           TEXT        NOT NULL,
    customer_type  TEXT        NOT NULL,              -- b2c
    loyalty_tier   TEXT        NOT NULL,              -- bronze | silver | gold
    created_at     TIMESTAMP   NOT NULL,
    updated_at     TIMESTAMP   NOT NULL
);

CREATE TABLE products (
    product_id      INT PRIMARY KEY,
    sku             TEXT          NOT NULL,
    name            TEXT          NOT NULL,
    category        TEXT          NOT NULL,
    brand           TEXT          NOT NULL,
    list_price_eur  NUMERIC(10,2) NOT NULL,
    created_at      TIMESTAMP     NOT NULL,
    updated_at      TIMESTAMP     NOT NULL
);

CREATE TABLE orders (
    order_id       BIGINT PRIMARY KEY,
    customer_id    INT          NOT NULL REFERENCES customers,
    order_ts       TIMESTAMP    NOT NULL,
    sales_channel  TEXT         NOT NULL,             -- web | app
    country        CHAR(2)      NOT NULL,
    currency       CHAR(3)      NOT NULL,             -- RSD | EUR
    status         TEXT         NOT NULL,             -- pending > paid > shipped > delivered | cancelled | returned
    updated_at     TIMESTAMP    NOT NULL
);

CREATE TABLE order_items (
    order_item_id  BIGINT PRIMARY KEY,
    order_id       BIGINT        NOT NULL REFERENCES orders,
    product_id     INT           NOT NULL REFERENCES products,
    quantity       INT           NOT NULL,
    unit_price     NUMERIC(10,2) NOT NULL,            -- u valuti porudzbine
    discount       NUMERIC(10,2) NOT NULL DEFAULT 0,  -- iznos popusta na stavku
    item_status    TEXT          NOT NULL,
    updated_at     TIMESTAMP     NOT NULL
);

CREATE INDEX ON customers (updated_at);
CREATE INDEX ON products (updated_at);
CREATE INDEX ON orders (updated_at);
CREATE INDEX ON order_items (updated_at);
CREATE INDEX ON order_items (order_id);
