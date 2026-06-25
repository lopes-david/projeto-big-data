{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('databricks_silver', 'fact_orders') }}
)

SELECT
    order_id,
    customer_unique_id,
    order_status,
    purchase_timestamp,
    approved_at,
    delivered_carrier_date,
    delivered_customer_date,
    estimated_delivery_date,
    approval_time_hours,
    delivery_time_days,
    delivery_delay_days,
    item_count
FROM source
