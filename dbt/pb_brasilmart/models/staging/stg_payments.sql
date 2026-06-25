{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('databricks_silver', 'fact_payments') }}
)

SELECT
    order_id,
    payment_sequential,
    payment_method,
    payment_installments,
    payment_value
FROM source
