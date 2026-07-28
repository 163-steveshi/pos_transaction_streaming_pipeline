
Use catalog dbs_external_another;
-- SHOW EXTERNAL LOCATIONS;
--  SHOW STORAGE CREDENTIALS;
-- CREATE EXTERNAL LOCATION pos_transaction_landing_loc
-- URL 's3://spark-read-study-bucket/pos_transaction_landing/'
-- WITH (STORAGE CREDENTIAL `db_s3_credentials_databricks-s3-ingest-50f99`);

-- in real industry this not enough, you also want kafak write raw achieves into s3
-- prevent error hapoen that fall out of the time travel time range (time  retention )

CREATE TABLE dbs_external_another.bronze.raw_pos_transaction(
  transaction_id String,
    item_sequence String,
    customer_id String,
    event_time String,
    sku String,
    item_unit_price String,
    item_quantity String,
    discount_amount String,
    tax_amount String,
    payment_type String,
    currency String,
    country String,
    channel String,
    store_id String,
    terminal_id String,
    ingested_time TIMESTAMP
)
USING DELTA LOCATION 's3://spark-read-study-bucket/pos_transaction_landing/'
TBLPROPERTIES (
  'delta.appendOnly' = 'true',
  'quality' ='bronze'
);

-- DESCRIBE EXTERNAL LOCATION `db_s3_external_databricks-s3-ingest-50f99`;
-- SELECT * FROM   dbs_external_another.bronze.raw_pos_transaction limit 5000;

-- old lakehouse: landing: raw file ---> bronze: querable table
--- new lakehouse: landing: bronze: use ice berg /delat lake format no old save an additional copy for the stream api type of responds
--kafka can have check points that serves the replay
--TODO: put into_another_folder_with the catlog and silver 
-- then try to add redirect to late data table
-- try to add the corrupt data into the dead letter queue
-- move clean data into scd2 and scd1 
CREATE OR REFRESH STREAMING TABLE silver_transactions
AS
SELECT
    transaction_id,
    CAST(item_quantity AS INT) AS item_quantity,
    CAST(item_unit_price AS DECIMAL(10,2)) AS item_unit_price,
    CAST(event_time AS TIMESTAMP) AS event_time,
    current_timestamp() AS processed_time
FROM STREAM(bronze_transactions);