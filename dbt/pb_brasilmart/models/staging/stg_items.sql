{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('databricks_silver', 'items') }}
)

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    total_item_value,
    _transformado_em
FROM source
