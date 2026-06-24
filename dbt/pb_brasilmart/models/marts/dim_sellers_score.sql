-- TABLE: score pré-calculado para dashboards de gestão de marketplace
-- DIST KEY: seller_id — chave primária, distribui uniformemente
-- SORT KEY: seller_tier, seller_score — otimiza ranking e filtros por tier
{{ config(
    materialized='table',
    dist='seller_id',
    sort=['seller_tier', 'seller_score']
) }}

WITH sellers AS (
    SELECT * FROM {{ ref('stg_sellers') }}
),

items AS (
    SELECT * FROM {{ ref('stg_items') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

reviews AS (
    SELECT * FROM {{ ref('stg_reviews') }}
),

seller_orders AS (
    SELECT
        i.seller_id,
        o.order_id,
        o.order_status,
        o.delta_entrega_dias,
        o.status_entrega
    FROM items i
    INNER JOIN orders o ON i.order_id = o.order_id
),

seller_reviews AS (
    SELECT
        i.seller_id,
        AVG(r.review_score) AS avg_review_score,
        COUNT(CASE WHEN r.review_score <= 2 THEN 1 END) AS negative_reviews,
        COUNT(r.review_id) AS total_reviews
    FROM items i
    INNER JOIN reviews r ON i.order_id = r.order_id
    GROUP BY i.seller_id
),

seller_metrics AS (
    SELECT
        so.seller_id,
        COUNT(DISTINCT so.order_id) AS total_orders,
        COUNT(CASE WHEN so.order_status = 'delivered' THEN 1 END) AS delivered_orders,
        COUNT(CASE WHEN so.order_status = 'canceled' THEN 1 END) AS canceled_orders,
        COUNT(CASE WHEN so.status_entrega = 'no_prazo' THEN 1 END) AS on_time_orders,
        AVG(so.delta_entrega_dias) AS avg_delivery_delta
    FROM seller_orders so
    GROUP BY so.seller_id
),

scored AS (
    SELECT
        s.seller_id,
        s.seller_city,
        s.seller_state,
        sm.total_orders,
        sm.delivered_orders,
        sm.canceled_orders,
        ROUND(sr.avg_review_score::DECIMAL(3,2), 2) AS avg_review_score,
        sr.negative_reviews,
        ROUND((sm.on_time_orders::DECIMAL / NULLIF(sm.delivered_orders, 0) * 100)::DECIMAL(5,2), 2) AS on_time_rate,
        ROUND((sm.canceled_orders::DECIMAL / NULLIF(sm.total_orders, 0) * 100)::DECIMAL(5,2), 2) AS cancel_rate,
        -- Score composto: reviews (40%) + on-time (30%) + cancel (20%) + volume (10%)
        ROUND((
            COALESCE(sr.avg_review_score / 5.0, 0.5) * 40 +
            COALESCE(sm.on_time_orders::DECIMAL / NULLIF(sm.delivered_orders, 0), 0.5) * 30 +
            (1 - COALESCE(sm.canceled_orders::DECIMAL / NULLIF(sm.total_orders, 0), 0)) * 20 +
            LEAST(sm.total_orders::DECIMAL / 100, 1) * 10
        )::DECIMAL(5,2), 2) AS seller_score,
        CURRENT_TIMESTAMP AS _gerado_em
    FROM sellers s
    LEFT JOIN seller_metrics sm ON s.seller_id = sm.seller_id
    LEFT JOIN seller_reviews sr ON s.seller_id = sr.seller_id
    WHERE sm.total_orders > 0
)

SELECT
    *,
    CASE
        WHEN seller_score >= 80 THEN 'Excelente'
        WHEN seller_score >= 60 THEN 'Bom'
        WHEN seller_score >= 40 THEN 'Regular'
        ELSE 'Critico'
    END AS seller_tier
FROM scored
