-- INCREMENTAL: só processa dias novos a cada execução
-- Primeira run: carga completa. Runs seguintes: apenas novos order_purchase_timestamp
-- DIST KEY: data_venda — consultas sempre filtram/agrupam por data
-- SORT KEY: data_venda — range scans eficientes em séries temporais
{{ config(
    materialized='incremental',
    unique_key='data_venda',
    dist='data_venda',
    sort='data_venda',
    incremental_strategy='delete+insert'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    {% if is_incremental() %}
    WHERE order_purchase_timestamp >= (SELECT DATEADD(day, -3, MAX(data_venda)) FROM {{ this }})
    {% endif %}
),

items AS (
    SELECT * FROM {{ ref('stg_items') }}
),

daily AS (
    SELECT
        DATE(o.order_purchase_timestamp) AS data_venda,
        COUNT(DISTINCT o.order_id) AS total_pedidos,
        COUNT(DISTINCT o.customer_id) AS total_clientes,
        ROUND(SUM(i.total_item_value)::DECIMAL(12,2), 2) AS gmv,
        ROUND(AVG(i.total_item_value)::DECIMAL(10,2), 2) AS ticket_medio,
        ROUND(SUM(i.freight_value)::DECIMAL(12,2), 2) AS total_frete,
        CURRENT_TIMESTAMP AS _gerado_em
    FROM orders o
    INNER JOIN items i ON o.order_id = i.order_id
    WHERE o.order_status IN ('delivered', 'shipped', 'invoiced', 'processing')
    GROUP BY DATE(o.order_purchase_timestamp)
)

SELECT * FROM daily
ORDER BY data_venda
