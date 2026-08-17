CREATE OR REFRESH STREAMING live TABLE customers_cleaned (
    CONSTRAINT valid_id
        EXPECT (customer_id IS NOT NULL)
        ON VIOLATION FAIL UPDATE,

    CONSTRAINT completed_profile
        EXPECT (
            first_name IS NOT NULL
            AND TRIM(first_name) != ''
            AND last_name IS NOT NULL
            AND TRIM(last_name) != ''
        )
        ON VIOLATION DROP ROW,

    CONSTRAINT realistic_age
        EXPECT (
            birth_date IS NULL
            OR birth_date BETWEEN '1900-01-01' AND current_date()
        )
        ON VIOLATION DROP ROW,

    CONSTRAINT no_upstream_failures
        EXPECT (SIZE(dq_failure_reasons) = 0)
        ON VIOLATION FAIL UPDATE
)
COMMENT 'Streaming Silver table enforcing strict data quality constraints on incoming streams'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    state,
    country,
    birth_date,
    gender,
    registration_date,
    loyalty_tier,
    is_active,
    updated_at,
    zip_code,
    ingestion_time,
    _rescued_data
FROM STREAM(dbs_external_another.bronze.customers_flagged)
WHERE SIZE(dq_failure_reasons) = 0;

CREATE OR REFRESH STREAMING LIVE TABLE customers_scd1
TBLPROPERTIES("quality" = "silver");

APPLY CHANGES INTO live.customers_scd1
FROM STREAM(silver.customers_cleaned)
KEYS (customer_id)                          
SEQUENCE BY ingestion_time         
COLUMNS * EXCEPT (_rescued_data)
STORED AS SCD TYPE 1;

CREATE OR REFRESH STREAMING LIVE TABLE customers_scd2
TBLPROPERTIES("quality" = "silver");

APPLY CHANGES INTO live.customers_scd2
FROM STREAM(silver.customers_cleaned)
KEYS (customer_id)                          
SEQUENCE BY ingestion_time         
COLUMNS * EXCEPT (_rescued_data)
STORED AS SCD TYPE 2;