{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('databricks_silver', 'dim_customers') }}
)

SELECT
    customer_unique_id,
    zip_code_prefix,
    city,
    state,
    latitude,
    longitude,
    order_count
FROM source
