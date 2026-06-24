-- TABLE: métricas de produto para equipe de catálogo
-- DIST KEY: product_id — chave primária, distribui uniformemente
-- SORT KEY: product_status, total_revenue — otimiza filtro por status e ranking por receita
{{ config(
    materialized='table',
    dist='product_id',
    sort=['product_status', 'total_revenue']
) }}

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
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

product_sales AS (
    SELECT
        i.product_id,
        COUNT(DISTINCT i.order_id) AS total_orders,
        SUM(i.total_item_value) AS total_revenue,
        AVG(i.price) AS avg_price,
        MAX(o.order_purchase_timestamp) AS last_sale_date,
        DATEDIFF(day, MAX(o.order_purchase_timestamp), CURRENT_DATE) AS days_since_last_sale
    FROM items i
    INNER JOIN orders o ON i.order_id = o.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY i.product_id
),

product_reviews AS (
    SELECT
        i.product_id,
        AVG(r.review_score) AS avg_review_score,
        COUNT(r.review_id) AS total_reviews,
        COUNT(CASE WHEN r.review_score <= 2 THEN 1 END) AS negative_reviews
    FROM items i
    INNER JOIN reviews r ON i.order_id = r.order_id
    GROUP BY i.product_id
),

final AS (
    SELECT
        p.product_id,
        p.product_category,
        p.product_weight_kg,
        ps.total_orders,
        ROUND(ps.total_revenue::DECIMAL(12,2), 2) AS total_revenue,
        ROUND(ps.avg_price::DECIMAL(10,2), 2) AS avg_price,
        ps.last_sale_date,
        ps.days_since_last_sale,
        ROUND(pr.avg_review_score::DECIMAL(3,2), 2) AS avg_review_score,
        pr.total_reviews,
        ROUND((pr.negative_reviews::DECIMAL / NULLIF(pr.total_reviews, 0) * 100)::DECIMAL(5,2), 2) AS negative_review_rate,
        CASE
            WHEN ps.days_since_last_sale <= 30 THEN 'Ativo'
            WHEN ps.days_since_last_sale <= 90 THEN 'Moderado'
            WHEN ps.days_since_last_sale <= 180 THEN 'Baixo'
            ELSE 'Inativo'
        END AS product_status,
        CURRENT_TIMESTAMP AS _gerado_em
    FROM products p
    LEFT JOIN product_sales ps ON p.product_id = ps.product_id
    LEFT JOIN product_reviews pr ON p.product_id = pr.product_id
    WHERE ps.total_orders > 0
)

SELECT * FROM final
