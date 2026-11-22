{{
    config(
        materialized='incremental',
        unique_key='product_id'
    )
}}

SELECT 
    cast(product_id as bigint)              as product_id,
    cast(product_name as string)            as product_name,
    cast(category as string)                as category,
    cast(brand as string)                   as brand,
    cast(price as decimal(10, 2))           as price,
    to_timestamp(created_timestamp)         as created_timestamp,
    to_timestamp(updated_timestamp)         as updated_timestamp,
    cast(is_active as string)               as is_active,
    current_timestamp()                     as processed_at
FROM {{ source('walmart_databricks', 'products')}}


{% if is_incremental() %}
    WHERE to_timestamp(updated_timestamp) > (
        SELECT COALESCE(MAX(updated_timestamp), to_timestamp('1900-01-01')) 
        FROM {{ this }}
    )
{% endif %}