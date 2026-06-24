-- TABLE: dimensão geográfica unificada (clientes + vendedores) para análises regionais
-- DIST KEY: estado — consultas frequentemente filtram/agrupam por UF
-- SORT KEY: estado, cidade — otimiza filtros hierárquicos (estado → cidade)
{{ config(
    materialized='table',
    dist='estado',
    sort=['estado', 'cidade']
) }}

WITH locais_clientes AS (
    SELECT DISTINCT
        customer_state AS estado,
        customer_city AS cidade
    FROM {{ ref('stg_customers') }}
    WHERE customer_state IS NOT NULL
),

locais_sellers AS (
    SELECT DISTINCT
        seller_state AS estado,
        seller_city AS cidade
    FROM {{ ref('stg_sellers') }}
    WHERE seller_state IS NOT NULL
),

locais_unificados AS (
    SELECT estado, cidade FROM locais_clientes
    UNION
    SELECT estado, cidade FROM locais_sellers
),

enriched AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY estado, cidade) AS localizacao_id,
        estado,
        cidade,
        CASE
            WHEN estado IN ('AM', 'RR', 'AP', 'PA', 'TO', 'RO', 'AC') THEN 'Norte'
            WHEN estado IN ('MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA') THEN 'Nordeste'
            WHEN estado IN ('MT', 'MS', 'GO', 'DF') THEN 'Centro-Oeste'
            WHEN estado IN ('SP', 'RJ', 'ES', 'MG') THEN 'Sudeste'
            WHEN estado IN ('PR', 'SC', 'RS') THEN 'Sul'
            ELSE 'Outros'
        END AS regiao,
        CASE
            WHEN estado IN ('SP', 'RJ', 'MG', 'RS', 'PR') THEN 'Tier 1'
            WHEN estado IN ('BA', 'SC', 'GO', 'PE', 'CE', 'DF', 'ES', 'PA') THEN 'Tier 2'
            ELSE 'Tier 3'
        END AS tier_mercado,
        CURRENT_TIMESTAMP AS _gerado_em
    FROM locais_unificados
)

SELECT * FROM enriched
