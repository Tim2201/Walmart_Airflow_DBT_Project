{{
    config(
        materialized='incremental',
        unique_key='employee_id'
    )
}}

SELECT 
    CAST(employee_id AS INT)                                   AS employee_id,
    CAST(store_id AS INT)                                      AS store_id,
    CAST(first_name AS STRING)                                 AS first_name,
    CAST(last_name AS STRING)                                  AS last_name,
    CAST(email AS STRING)                                      AS email,
    CAST(job_title AS STRING)                                  AS job_title,
    CAST(salary AS DECIMAL(10, 2))                             AS salary,
    -- 若來源為字串（如 2026/5/30 16:26），加上格式化字串解析以避免報錯
    to_timestamp(created_timestamp)           AS created_timestamp,
    to_timestamp(updated_timestamp)           AS updated_timestamp,
    CAST(is_active AS STRING)                                  AS is_active,
    current_timestamp()                                        AS processed_at
FROM {{ source('walmart_databricks', 'employees') }}


{% if is_incremental() %}
    WHERE to_timestamp(updated_timestamp) > (
        SELECT COALESCE(MAX(updated_timestamp), to_timestamp('1900-01-01')) 
        FROM {{ this }}
    )
{% endif %}