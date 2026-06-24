{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('databricks_silver', 'payments') }}
)

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    payment_group,
    _transformado_em
FROM source
