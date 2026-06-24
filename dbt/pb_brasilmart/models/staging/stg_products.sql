{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('databricks_silver', 'products') }}
)

SELECT
    product_id,
    product_category,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_kg,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_volume_cm3,
    porte_produto,
    _transformado_em
FROM source
