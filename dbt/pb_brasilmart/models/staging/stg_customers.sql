{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('databricks_silver', 'customers') }}
)

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code,
    customer_city,
    customer_state,
    _transformado_em
FROM source
