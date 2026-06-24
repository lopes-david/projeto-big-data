-- TABLE: features pré-agregadas no nível de pedido para consumo do modelo de ML (Databricks)
-- Replica a feature engineering do TP4 (predição de atraso) em SQL/dbt
-- DIST KEY: order_id — chave primária, otimiza leitura bulk pelo Databricks
-- SORT KEY: label — agrupa atrasados/no_prazo para scans eficientes
{{ config(
    materialized='table',
    dist='order_id',
    sort='label'
) }}

WITH orders_enriched AS (
    SELECT * FROM {{ source('databricks_silver', 'orders_enriched') }}
),

items_enriched AS (
    SELECT * FROM {{ source('databricks_silver', 'items_enriched') }}
),

items_agg AS (
    SELECT
        order_id,
        SUM(total_item_value) AS total_itens_valor,
        SUM(freight_value) AS total_frete,
        AVG(product_weight_kg) AS peso_medio_kg,
        AVG(product_volume_cm3) AS volume_medio_cm3,
        COUNT(*) AS qtd_itens,
        MAX(seller_state) AS seller_state
    FROM items_enriched
    GROUP BY order_id
),

features AS (
    SELECT
        o.order_id,
        o.customer_id,

        -- Target: 1 = atrasado, 0 = no_prazo
        CASE WHEN o.status_entrega = 'atrasado' THEN 1 ELSE 0 END AS label,

        -- Features temporais
        COALESCE(o.tempo_aprovacao_seg, 0) AS tempo_aprovacao_seg,
        COALESCE(o.tempo_postagem_seg, 0) AS tempo_postagem_seg,

        -- Features financeiras do pedido
        COALESCE(o.total_pago, 0) AS total_pago,
        COALESCE(o.max_parcelas, 1) AS max_parcelas,

        -- Features de pagamento (one-hot encoding)
        CASE WHEN o.grupo_pagamento_principal = 'cartao' THEN 1 ELSE 0 END AS pag_cartao,
        CASE WHEN o.grupo_pagamento_principal = 'boleto' THEN 1 ELSE 0 END AS pag_boleto,

        -- Feature geográfica do cliente
        o.customer_state,

        -- Features agregadas dos itens
        COALESCE(ROUND(i.total_itens_valor::DECIMAL(12,2), 2), 0) AS total_itens_valor,
        COALESCE(ROUND(i.total_frete::DECIMAL(12,2), 2), 0) AS total_frete,
        COALESCE(ROUND(i.peso_medio_kg::DECIMAL(10,4), 4), 0) AS peso_medio_kg,
        COALESCE(ROUND(i.volume_medio_cm3::DECIMAL(12,2), 2), 0) AS volume_medio_cm3,
        COALESCE(i.qtd_itens, 1) AS qtd_itens,

        -- Feature geográfica do vendedor
        i.seller_state,

        -- Metadados (não usados como feature, úteis para análise)
        o.delta_entrega_dias,
        o.status_entrega,
        o.order_status,

        CURRENT_TIMESTAMP AS _gerado_em

    FROM orders_enriched o
    INNER JOIN items_agg i ON o.order_id = i.order_id
    WHERE o.status_entrega IN ('no_prazo', 'atrasado')
      AND o.tempo_aprovacao_seg IS NOT NULL
)

SELECT * FROM features
