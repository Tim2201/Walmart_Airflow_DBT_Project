{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

SELECT 
    cast(order_id as bigint)                as order_id,
    cast(customer_id as bigint)             as customer_id,
    cast(store_id as bigint)                as store_id,
    to_timestamp(order_timestamp)           as order_timestamp,
    cast(payment_method as string)          as payment_method,
    cast(order_status as string)            as order_status,
    cast(total_amount as decimal(10, 2))    as total_amount,
    to_timestamp(created_timestamp)         as created_timestamp,
    to_timestamp(updated_timestamp)         as updated_timestamp,
    cast(is_active as string)               as is_active,
    current_timestamp()                     as processed_at
FROM {{ source('walmart_databricks', 'orders')}}


{% if is_incremental() %}
    WHERE to_timestamp(updated_timestamp) > (
        SELECT COALESCE(MAX(updated_timestamp), to_timestamp('1900-01-01')) 
        FROM {{ this }}
    )
{% endif %}