SELECT
    DISTINCT
    product_id,
    product_name,
    price,
    category,
    brand,
    product_created_timestamp,
    product_updated_timestamp,
    product_is_active,
    product_processed_at,
    CURRENT_TIMESTAMP() AS product_gold_processed_at
FROM
    {{ ref('obt_b') }}