{{
    config(
        materialized='incremental',
        unique_key='order_item_id'
    )
}}

SELECT 
    CAST(order_item_id AS INT)                 AS order_item_id,
    CAST(order_id AS INT)                      AS order_id,
    CAST(product_id AS INT)                    AS product_id,
    CAST(quantity AS INT)                      AS quantity,
    CAST(unit_price AS DECIMAL(10, 2))         AS unit_price,
    CAST(line_amount AS DECIMAL(10, 2))        AS line_amount,
    to_timestamp(created_timestamp)            AS created_timestamp,
    to_timestamp(updated_timestamp)            AS updated_timestamp,
    CAST(is_active AS STRING)                  AS is_active,
    current_timestamp()                        AS processed_at
FROM {{ source('walmart_databricks', 'order_items') }}

{% if is_incremental() %}
    WHERE to_timestamp(updated_timestamp) > (
        SELECT COALESCE(MAX(updated_timestamp), to_timestamp('1900-01-01')) 
        FROM {{ this }}
    )
{% endif %}