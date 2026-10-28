CREATE OR REPLACE VIEW dbs_external_another.gold.sales_metrics
WITH METRICS LANGUAGE YAML
AS $$
version: 1.1
comment: "Retail sales semantic model at transaction-line grain"
source: dbs_external_another.gold.fct_sales_line
dimensions:
  # --- Time ---
  - name: Order Date
    expr: event_date
  - name: Order Month
    expr: DATE_TRUNC('MONTH', event_date)
  - name: Day of Week
    expr: DATE_FORMAT(event_date, 'EEE')
  - name: Hour of Day
    expr: HOUR(event_time)
 
  # --- Product ---
  - name: SKU
    expr: sku
 
  # --- Location / sales context ---
  - name: Country
    expr: country
  - name: Channel
    expr: channel
  - name: Store
    expr: store_id
  - name: Terminal
    expr: terminal_id
  - name: Payment Type
    expr: payment_type
  - name: Currency
    expr: currency
  - name: Is Return
    expr: is_return
 
  # --- Customer ---
  - name: Customer Type
    expr: CASE WHEN customer_id IS NULL THEN 'Guest' ELSE 'Registered' END
measures:
  # --- Revenue ---
  - name: Gross Sales
    expr: SUM(gross_amount)
  - name: Discounts
    expr: SUM(discount_amount)
  - name: Net Sales
    expr: SUM(net_amount)
  - name: Tax
    expr: SUM(tax_amount)
  - name: Total Sales
    expr: SUM(total_amount)
  - name: Transactions
    expr: COUNT(DISTINCT transaction_id)
  - name: Units Sold
    expr: SUM(item_quantity) FILTER (WHERE NOT is_return)
  - name: Units Returned
    expr: -SUM(item_quantity) FILTER (WHERE is_return)
  - name: Avg Basket Value
    expr: SUM(total_amount) / COUNT(DISTINCT transaction_id)
    
  # --- Volume ---
  - name: Orders
    expr: COUNT(DISTINCT transaction_id)
  - name: Customers
    expr: COUNT(DISTINCT customer_id)
 
  # --- Ratios (defined once, correct at any grouping level) ---
  - name: Avg Order Value
    expr: SUM(net_amount) / NULLIF(COUNT(DISTINCT transaction_id), 0)
  - name: Units per Order
    expr: SUM(item_quantity) / NULLIF(COUNT(DISTINCT transaction_id), 0)
  - name: Avg Selling Price
    expr: SUM(net_amount) / NULLIF(SUM(item_quantity), 0)
  - name: Discount Rate
    expr: SUM(discount_amount) / NULLIF(SUM(gross_amount), 0)
  - name: Return Rate
    expr: >
      COUNT(DISTINCT CASE WHEN is_return THEN transaction_id END)
      / NULLIF(COUNT(DISTINCT transaction_id), 0)
$$;

-- if the model source is a regular table we sometime need materalization for 
-- cache respond instead of compute every time
-- materialization:
--   schedule: every 1 hour          # run after fct_sales_line refreshes
--   mode: relaxed
--   materialized_views:
--     - name: monthly_country_channel
--       type: aggregated
--       dimensions:
--         - Order Month
--         - Country
--         - Channel
--         - Currency                # include filter columns too
--       measures:
--         - Net Sales
--         - Gross Sales
--         - Discounts
--         - Orders
--         - Avg Order Value
--     - name: daily_store
--       type: aggregated
--       dimensions:
--         - Order Date
--         - Store
--         - Currency
--       measures:
--         - Net Sales
--         - Orders