CREATE OR REFRESH STREAMING LIVE TABLE customers_flagged
COMMENT "Intermediate table: casts + flags validity, nothing is dropped here"
TBLPROPERTIES ("quality" = "bronze")
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
    TRY_CAST(birth_date AS date) AS birth_date,
    gender,
    TRY_CAST(registration_date AS timestamp) AS registration_date,
    loyalty_tier,
    TRY_CAST(is_active AS boolean) AS is_active,
    TRY_CAST(updated_at AS timestamp) AS updated_at,
    TRY_CAST(zip_code AS STRING) AS zip_code,
    current_timestamp() AS ingestion_time,
    _rescued_data,

    -- one boolean check per rule, each independently evaluated
    FILTER(
      ARRAY(
        CASE WHEN customer_id IS NULL THEN 'miss_customer_id' END,
        CASE WHEN first_name IS NULL THEN 'miss_first_name' END,
        CASE WHEN last_name IS NULL THEN 'miss_last_name' END,
        CASE WHEN email IS NULL THEN 'miss_email' END,
        CASE WHEN email IS NOT NULL 
             AND email NOT RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' 
             THEN 'invalid_email' END,
        CASE WHEN phone IS NULL THEN 'miss_phone' END,
        CASE WHEN phone IS NOT NULL 
             AND phone NOT RLIKE '^\\+?\\(?[0-9]{1,3}\\)?[-. ]?\\(?[0-9]{3}\\)?[-. ]?[0-9]{3}[-. ]?[0-9]{4}(\\s?(x|ext\\.?)\\s?[0-9]{1,6})?$' 
             THEN 'invalid_phone' END,
        CASE WHEN address IS NULL THEN 'miss_address' END,
        CASE WHEN address IS NOT NULL 
             AND address NOT RLIKE '^[0-9]+[A-Za-z0-9 .,#-]*$' 
             THEN 'invalid_address' END,
        CASE WHEN city IS NULL THEN 'miss_city' END,
        CASE WHEN state IS NULL THEN 'miss_state' END,
        CASE WHEN country IS NULL THEN 'miss_country' END,
        CASE WHEN birth_date IS NULL THEN 'miss_birth_date' END,
        CASE WHEN birth_date IS NOT NULL 
             AND TRY_CAST(birth_date AS date) IS NULL 
             THEN 'birth_date_malformed' END,
        CASE WHEN gender IS NULL THEN 'miss_gender' END,
        CASE WHEN gender IS NOT NULL 
             AND gender NOT IN ('M', 'F') 
             THEN 'invalid_gender' END,
        CASE WHEN registration_date IS NULL THEN 'miss_registration_date' END,
        CASE WHEN registration_date IS NOT NULL 
             AND TRY_CAST(registration_date AS timestamp) IS NULL 
             THEN 'registration_date_malformed' END,
        CASE WHEN loyalty_tier IS NULL THEN 'miss_loyalty_tier' END,
        CASE WHEN loyalty_tier IS NOT NULL 
             AND loyalty_tier NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum') 
             THEN 'invalid_loyalty_tier' END,
        CASE WHEN is_active IS NULL THEN 'miss_is_active' END,
        CASE WHEN is_active IS NOT NULL 
             AND TRY_CAST(is_active AS boolean) IS NULL 
             THEN 'is_active_malformed' END,
        CASE WHEN updated_at IS NULL THEN 'miss_updated_at' END,
        CASE WHEN updated_at IS NOT NULL 
             AND TRY_CAST(updated_at AS timestamp) IS NULL 
             THEN 'updated_at_malformed' END,
        CASE WHEN zip_code IS NULL THEN 'miss_zip_code' END,
        CASE WHEN zip_code IS NOT NULL 
             AND CAST(zip_code AS STRING) NOT RLIKE '^[0-9]{5}(-[0-9]{4})?$' 
             THEN 'invalid_zip_code' END
      ),
      x -> x IS NOT NULL
    ) AS dq_failure_reasons

FROM STREAM(bronze.raw_customers_sltl);





CREATE OR REFRESH STREAMING LIVE TABLE customers_dlq
COMMENT "Dead-letter table capturing rows that failed data quality checks"
TBLPROPERTIES("quality" = "quarantine")
AS
SELECT *, true as is_corrupted
FROM STREAM(bronze.customers_flagged)
WHERE SIZE(dq_failure_reasons) > 0;
