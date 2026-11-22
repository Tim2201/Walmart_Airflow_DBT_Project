{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}

SELECT 
    CAST(customer_id AS INT)                   AS customer_id,
    CAST(first_name AS STRING)                 AS first_name,
    CAST(last_name AS STRING)                  AS last_name,
    CAST(email AS STRING)                      AS email,
    CAST(phone AS STRING)                      AS phone,
    CAST(city AS STRING)                       AS city,
    CAST(province AS STRING)                   AS province,
    CAST(country AS STRING)                    AS country,
    to_timestamp(created_timestamp)            AS created_timestamp,
    to_timestamp(updated_timestamp)            AS updated_timestamp,
    CAST(is_active AS STRING)                  AS is_active,
    current_timestamp()                        AS processed_at
FROM {{ source('walmart_databricks', 'customers') }}


{% if is_incremental() %}
    WHERE to_timestamp(updated_timestamp) > (
        SELECT COALESCE(MAX(updated_timestamp), to_timestamp('1900-01-01')) 
        FROM {{ this }}
    )
{% endif %}