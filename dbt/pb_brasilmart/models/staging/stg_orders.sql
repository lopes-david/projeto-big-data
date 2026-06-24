{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('databricks_silver', 'orders') }}
)

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    tempo_aprovacao_seg,
    tempo_postagem_seg,
    tempo_transporte_seg,
    tempo_total_seg,
    delta_entrega_dias,
    status_entrega,
    _transformado_em
FROM source
