CREATE OR REPLACE TABLE pos_transactions (
    transaction_id STRING,
    item_sequence INT,
    customer_id BIGINT,
    event_time TIMESTAMP,
    sku STRING,
    item_unit_price STRING,
    item_quantity DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    payment_type STRING,
    currency STRING,
    country STRING,
    channel STRING,
    store_id STRING,
    terminal_id STRING,
    ingested_time TIMESTAMP
);

CREATE OR REPLACE TABLE pos_transactions_late_arrivals (
    transaction_id STRING,
    item_sequence INT,
    customer_id BIGINT,
    event_time TIMESTAMP,
    sku STRING,
    item_unit_price STRING,
    item_quantity DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    payment_type STRING,
    currency STRING,
    country STRING,
    channel STRING,
    store_id STRING,
    terminal_id STRING,
    ingested_time TIMESTAMP,
    is_late BOOLEAN
);



CREATE OR REPLACE TABLE