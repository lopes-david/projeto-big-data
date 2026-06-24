{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('databricks_silver', 'sellers') }}
)

SELECT
    seller_id,
    seller_zip_code,
    seller_city,
    seller_state,
    _transformado_em
FROM source
