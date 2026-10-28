-- understand the data before building anything on it. It’s called data profiling
SELECT
  COUNT(*)                                             AS total_lines,
  COUNT(DISTINCT transaction_id)                       AS total_orders,
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS guest_lines,
  SUM(CASE WHEN item_quantity < 0 THEN 1 ELSE 0 END)   AS return_lines,
  COUNT(DISTINCT currency)                             AS currencies,
  MIN(event_time)                                      AS first_event,
  MAX(event_time)                                      AS last_event
FROM dbs_external_another.silver.pos_transactions;


CREATE OR REPLACE MATERIALIZED VIEW dbs_external_another.gold.fct_sales_line
SCHEDULE EVERY 24 HOUR
AS
SELECT
  transaction_id,
  item_sequence,
  customer_id,
  event_time,
  CAST(event_time AS DATE)                                        AS event_date,
  sku,
  item_unit_price,
  item_quantity,
  CAST(item_unit_price * item_quantity AS DECIMAL(18, 2))         AS gross_amount,
  discount_amount,
  CAST(item_unit_price * item_quantity - discount_amount
       AS DECIMAL(18, 2))                                         AS net_amount,
  tax_amount,
  CAST(item_unit_price * item_quantity - discount_amount + tax_amount
       AS DECIMAL(18, 2))                                         AS total_amount,
  item_quantity < 0                                               AS is_return,
  payment_type,
  currency,
  country,
  channel,
  store_id,
  terminal_id,
  ingested_time
FROM dbs_external_another.silver.pos_transactions;
 


 SELECt * FROM dbs_external_another.gold.fct_sales_line LIMIT 1000;