CREATE OR REPLACE TABLE dbs_external_another.silver.pos_transactions (
    transaction_id STRING NOT NULL,
    item_sequence INT NOT NULL,
    customer_id STRING,
    event_time TIMESTAMP NOT NULL,
    sku STRING NOT NULL,
    item_unit_price DECIMAL(10,2) NOT NULL,
    item_quantity DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) NOT NULL,
    tax_amount DECIMAL(10,2)NOT NULL,
    payment_type STRING NOT NULL,
    currency STRING NOT NULL,
    country STRING NOT NULL,
    channel STRING NOT NULL,
    store_id STRING NOT NULL,
    terminal_id STRING NOT NULL,
    ingested_time TIMESTAMP NOT NULL
)USING DELTA LOCATION 's3://spark-read-study-bucket/pos_transaction_silver/cleaned_table'
TBLPROPERTIES (
  'quality' ='silver'
);
drop TABLE dbs_external_another.silver.pos_transactions;

CREATE EXTERNAL LOCATION pos_transaction_silver_loc
URL 's3://spark-read-study-bucket/pos_transaction_silver/'
WITH (STORAGE CREDENTIAL `db_s3_credentials_databricks-s3-ingest-50f99`);

drop TABLE dbs_external_another.silver.pos_transactions_late_arrivals;

CREATE OR REPLACE TABLE dbs_external_another.silver.pos_transactions_late_arrivals (
    transaction_id STRING NOT NULL,
    item_sequence INT NOT NULL,
    customer_id STRING,
    event_time TIMESTAMP NOT NULL,
    sku STRING NOT NULL,
    item_unit_price DECIMAL(10,2) NOT NULL,
    item_quantity DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) NOT NULL,
    tax_amount DECIMAL(10,2)NOT NULL,
    payment_type STRING NOT NULL,
    currency STRING NOT NULL,
    country STRING NOT NULL,
    channel STRING NOT NULL,
    store_id STRING NOT NULL,
    terminal_id STRING NOT NULL,
    ingested_time TIMESTAMP NOT NULL,
    is_late BOOLEAN NOT NULL,
    ingestion_delay_seconds BIGINT NOT NULL
)
USING DELTA LOCATION 's3://spark-read-study-bucket/pos_transaction_silver/late_table'
TBLPROPERTIES (
  'delta.appendOnly' = 'true',
  'quality' ='silver'
);
SELECT * FROM dbs_external_another.silver.pos_transactions_late_arrivals LIMIT 1000;
SELECT * FROM dbs_external_another.silver.pos_transactions LIMIT 1000;

