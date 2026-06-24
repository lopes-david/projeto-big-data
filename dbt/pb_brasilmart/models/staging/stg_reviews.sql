{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('databricks_silver', 'reviews') }}
)

SELECT
    review_id,
    order_id,
    review_score,
    review_title,
    review_message,
    review_creation_date,
    review_answer_timestamp,
    tempo_resposta_seg,
    review_sentiment,
    has_comment,
    _transformado_em
FROM source
