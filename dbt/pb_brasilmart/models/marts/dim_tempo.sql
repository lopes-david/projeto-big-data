-- TABLE: dimensão de tempo para análises temporais e sazonalidade
-- DIST KEY: data_id — distribui uniformemente (uma linha por dia)
-- SORT KEY: data_id — range scans eficientes em séries temporais
{{ config(
    materialized='table',
    dist='data_id',
    sort='data_id'
) }}

WITH date_spine AS (
    SELECT
        DISTINCT DATE(order_purchase_timestamp) AS data_id
    FROM {{ ref('stg_orders') }}
    WHERE order_purchase_timestamp IS NOT NULL
),

enriched AS (
    SELECT
        data_id,
        EXTRACT(YEAR FROM data_id) AS ano,
        EXTRACT(MONTH FROM data_id) AS mes,
        EXTRACT(DAY FROM data_id) AS dia,
        EXTRACT(QUARTER FROM data_id) AS trimestre,
        EXTRACT(DOW FROM data_id) AS dia_semana_num,
        CASE EXTRACT(DOW FROM data_id)
            WHEN 0 THEN 'Domingo'
            WHEN 1 THEN 'Segunda'
            WHEN 2 THEN 'Terça'
            WHEN 3 THEN 'Quarta'
            WHEN 4 THEN 'Quinta'
            WHEN 5 THEN 'Sexta'
            WHEN 6 THEN 'Sábado'
        END AS dia_semana_nome,
        CASE
            WHEN EXTRACT(DOW FROM data_id) IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_fim_de_semana,
        CASE EXTRACT(MONTH FROM data_id)
            WHEN 1 THEN 'Janeiro'
            WHEN 2 THEN 'Fevereiro'
            WHEN 3 THEN 'Março'
            WHEN 4 THEN 'Abril'
            WHEN 5 THEN 'Maio'
            WHEN 6 THEN 'Junho'
            WHEN 7 THEN 'Julho'
            WHEN 8 THEN 'Agosto'
            WHEN 9 THEN 'Setembro'
            WHEN 10 THEN 'Outubro'
            WHEN 11 THEN 'Novembro'
            WHEN 12 THEN 'Dezembro'
        END AS mes_nome,
        EXTRACT(YEAR FROM data_id)::VARCHAR || '-Q' || EXTRACT(QUARTER FROM data_id)::VARCHAR AS ano_trimestre,
        EXTRACT(YEAR FROM data_id)::VARCHAR || '-' || LPAD(EXTRACT(MONTH FROM data_id)::VARCHAR, 2, '0') AS ano_mes,
        CASE
            WHEN EXTRACT(MONTH FROM data_id) IN (11, 12, 1, 2) THEN 'Alta'
            WHEN EXTRACT(MONTH FROM data_id) IN (5, 6, 7, 8) THEN 'Baixa'
            ELSE 'Media'
        END AS sazonalidade_varejo,
        CURRENT_TIMESTAMP AS _gerado_em
    FROM date_spine
)

SELECT * FROM enriched
ORDER BY data_id
