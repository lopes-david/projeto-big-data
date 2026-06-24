-- TABLE: dados pré-computados para consultas rápidas de segmentação
-- DIST KEY: customer_unique_id — distribui uniformemente entre nós (chave de JOIN)
-- SORT KEY: rfm_segment, recency_days — otimiza filtros por segmento e ordenação por recência
{{ config(
    materialized='table',
    dist='customer_unique_id',
    sort=['rfm_segment', 'recency_days']
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

items AS (
    SELECT * FROM {{ ref('stg_items') }}
),

order_values AS (
    SELECT
        order_id,
        SUM(total_item_value) AS order_total
    FROM items
    GROUP BY order_id
),

rfm AS (
    SELECT
        c.customer_unique_id,
        c.customer_state,
        c.customer_city,
        COUNT(DISTINCT o.order_id) AS frequency,
        ROUND(SUM(ov.order_total)::DECIMAL(12,2), 2) AS monetary,
        DATEDIFF(day, MAX(o.order_purchase_timestamp), CURRENT_DATE) AS recency_days,
        MIN(o.order_purchase_timestamp) AS first_purchase,
        MAX(o.order_purchase_timestamp) AS last_purchase,
        ROUND(AVG(ov.order_total)::DECIMAL(10,2), 2) AS avg_ticket
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    INNER JOIN order_values ov ON o.order_id = ov.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id, c.customer_state, c.customer_city
),

segmented AS (
    SELECT
        *,
        CASE
            WHEN frequency >= 3 AND recency_days <= 90 THEN 'Champions'
            WHEN frequency >= 2 AND recency_days <= 180 THEN 'Loyal'
            WHEN frequency = 1 AND recency_days <= 90 THEN 'New'
            WHEN frequency >= 2 AND recency_days > 180 THEN 'At Risk'
            WHEN frequency = 1 AND recency_days > 365 THEN 'Lost'
            ELSE 'Hibernating'
        END AS rfm_segment,
        CURRENT_TIMESTAMP AS _gerado_em
    FROM rfm
)

SELECT * FROM segmented
