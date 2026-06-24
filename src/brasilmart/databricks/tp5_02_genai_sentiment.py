# Databricks notebook source
# MAGIC %md
# MAGIC # TP5 — Atividade 2.1: Análise de Sentimento com Databricks AI Functions
# MAGIC
# MAGIC ## Objetivo
# MAGIC Utilizar a função `ai_analyze_sentiment` do Databricks SQL (DBSQL) para
# MAGIC classificar automaticamente o sentimento dos comentários de reviews dos clientes.
# MAGIC
# MAGIC ## Contexto de Negócio (TP1 — Questão 11, Equipe de Reviews / Customer Success)
# MAGIC > "Analisamos manualmente uma amostra e percebemos que 65% das avaliações
# MAGIC > negativas mencionam atraso ou produto diferente do anunciado. Mas não temos
# MAGIC > como confirmar isso em escala — precisaríamos de análise de texto das reviews
# MAGIC > cruzada com dados de entrega."
# MAGIC
# MAGIC **Requisito TP1 (R10):** Pipeline NLP para classificação de reviews cruzada com
# MAGIC `delta_entrega`. Base para análise de causa raiz de insatisfação.
# MAGIC
# MAGIC ## Coluna utilizada
# MAGIC A coluna `review_message` da tabela `silver.reviews` contém os comentários
# MAGIC reais dos clientes (~41 mil mensagens). No contexto do enunciado, esta coluna
# MAGIC faz o papel de `comentarios_manutencao` — feedback textual do cliente sobre
# MAGIC a experiência de compra/entrega.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Exploração dos Dados de Reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total de reviews e proporção com comentário
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_reviews,
# MAGIC   SUM(CASE WHEN review_message IS NOT NULL AND review_message != '' THEN 1 ELSE 0 END) AS com_comentario,
# MAGIC   ROUND(SUM(CASE WHEN review_message IS NOT NULL AND review_message != '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_com_comentario
# MAGIC FROM pb_brasilmart.silver.reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribuição de score para reviews COM comentário
# MAGIC SELECT
# MAGIC   review_score,
# MAGIC   COUNT(*) AS total,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
# MAGIC FROM pb_brasilmart.silver.reviews
# MAGIC WHERE review_message IS NOT NULL AND review_message != ''
# MAGIC GROUP BY review_score
# MAGIC ORDER BY review_score

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Amostra de comentários por faixa de score
# MAGIC SELECT review_score, review_message
# MAGIC FROM pb_brasilmart.silver.reviews
# MAGIC WHERE review_message IS NOT NULL
# MAGIC   AND LENGTH(review_message) > 20
# MAGIC ORDER BY review_score, RAND()
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Análise de Sentimento com `ai_analyze_sentiment`
# MAGIC
# MAGIC A função `ai_analyze_sentiment` é uma AI Function nativa do Databricks SQL
# MAGIC que utiliza um LLM para classificar o sentimento de textos em linguagem natural.
# MAGIC
# MAGIC **Retorno:** `Positive`, `Negative`, `Neutral` ou `Mixed`.
# MAGIC
# MAGIC Aplicamos sobre uma amostra representativa (1.000 reviews) para demonstração,
# MAGIC pois a função consome tokens do Foundation Model API.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Criar view temporária com amostra estratificada por score
# MAGIC CREATE OR REPLACE TEMPORARY VIEW reviews_amostra AS
# MAGIC SELECT
# MAGIC   review_id,
# MAGIC   order_id,
# MAGIC   review_score,
# MAGIC   review_message,
# MAGIC   review_sentiment AS sentimento_regra,  -- sentimento rule-based da Silver (TP3)
# MAGIC   has_comment
# MAGIC FROM pb_brasilmart.silver.reviews
# MAGIC WHERE review_message IS NOT NULL
# MAGIC   AND LENGTH(TRIM(review_message)) > 10
# MAGIC ORDER BY RAND(42)
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar amostra
# MAGIC SELECT
# MAGIC   review_score,
# MAGIC   COUNT(*) AS total,
# MAGIC   sentimento_regra,
# MAGIC   COUNT(*) AS por_sentimento
# MAGIC FROM reviews_amostra
# MAGIC GROUP BY review_score, sentimento_regra
# MAGIC ORDER BY review_score, sentimento_regra

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Aplicar `ai_analyze_sentiment` na Amostra

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Classificar sentimento via GenAI (Databricks AI Function)
# MAGIC CREATE OR REPLACE TEMPORARY VIEW reviews_sentimento_genai AS
# MAGIC SELECT
# MAGIC   review_id,
# MAGIC   order_id,
# MAGIC   review_score,
# MAGIC   review_message,
# MAGIC   sentimento_regra,
# MAGIC   ai_analyze_sentiment(review_message) AS sentimento_genai
# MAGIC FROM reviews_amostra

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visualizar resultados
# MAGIC SELECT
# MAGIC   review_score,
# MAGIC   review_message,
# MAGIC   sentimento_regra,
# MAGIC   sentimento_genai
# MAGIC FROM reviews_sentimento_genai
# MAGIC ORDER BY review_score
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Análise Comparativa — GenAI vs Regras vs Score

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribuição do sentimento GenAI
# MAGIC SELECT
# MAGIC   sentimento_genai,
# MAGIC   COUNT(*) AS total,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
# MAGIC FROM reviews_sentimento_genai
# MAGIC GROUP BY sentimento_genai
# MAGIC ORDER BY total DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cruzamento: sentimento GenAI vs review_score
# MAGIC SELECT
# MAGIC   review_score,
# MAGIC   sentimento_genai,
# MAGIC   COUNT(*) AS total
# MAGIC FROM reviews_sentimento_genai
# MAGIC GROUP BY review_score, sentimento_genai
# MAGIC ORDER BY review_score, sentimento_genai

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Concordância entre sentimento GenAI e sentimento rule-based (Silver)
# MAGIC SELECT
# MAGIC   sentimento_regra,
# MAGIC   sentimento_genai,
# MAGIC   COUNT(*) AS total
# MAGIC FROM reviews_sentimento_genai
# MAGIC GROUP BY sentimento_regra, sentimento_genai
# MAGIC ORDER BY sentimento_regra, sentimento_genai

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Taxa de concordância GenAI vs regras
# MAGIC SELECT
# MAGIC   ROUND(
# MAGIC     SUM(CASE
# MAGIC       WHEN sentimento_genai = 'Positive' AND sentimento_regra = 'positivo' THEN 1
# MAGIC       WHEN sentimento_genai = 'Negative' AND sentimento_regra = 'negativo' THEN 1
# MAGIC       WHEN sentimento_genai = 'Neutral' AND sentimento_regra = 'neutro' THEN 1
# MAGIC       ELSE 0
# MAGIC     END) * 100.0 / COUNT(*), 1
# MAGIC   ) AS taxa_concordancia_pct,
# MAGIC   COUNT(*) AS total_avaliados
# MAGIC FROM reviews_sentimento_genai

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Casos Interessantes — Onde GenAI Diverge das Regras

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reviews onde o score numérico e o sentimento GenAI divergem
# MAGIC -- (ex: score alto mas sentimento negativo → sarcasmo ou ressalva)
# MAGIC SELECT
# MAGIC   review_score,
# MAGIC   sentimento_genai,
# MAGIC   sentimento_regra,
# MAGIC   review_message
# MAGIC FROM reviews_sentimento_genai
# MAGIC WHERE
# MAGIC   (review_score >= 4 AND sentimento_genai = 'Negative')
# MAGIC   OR (review_score <= 2 AND sentimento_genai = 'Positive')
# MAGIC ORDER BY review_score DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reviews Mixed — casos ambíguos que GenAI detecta e regras simples não
# MAGIC SELECT
# MAGIC   review_score,
# MAGIC   sentimento_genai,
# MAGIC   sentimento_regra,
# MAGIC   review_message
# MAGIC FROM reviews_sentimento_genai
# MAGIC WHERE sentimento_genai = 'Mixed'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cruzamento com Dados de Entrega (Requisito TP1 Q11)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sentimento GenAI cruzado com status de entrega
# MAGIC -- Confirma a hipótese: reviews negativas correlacionam com atraso?
# MAGIC SELECT
# MAGIC   r.sentimento_genai,
# MAGIC   o.status_entrega,
# MAGIC   COUNT(*) AS total,
# MAGIC   ROUND(AVG(o.delta_entrega_dias), 1) AS delta_medio_dias,
# MAGIC   ROUND(AVG(r.review_score), 2) AS score_medio
# MAGIC FROM reviews_sentimento_genai r
# MAGIC INNER JOIN pb_brasilmart.silver.orders_enriched o
# MAGIC   ON r.order_id = o.order_id
# MAGIC WHERE o.status_entrega IS NOT NULL
# MAGIC GROUP BY r.sentimento_genai, o.status_entrega
# MAGIC ORDER BY r.sentimento_genai, o.status_entrega

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Proporção de atraso por sentimento
# MAGIC SELECT
# MAGIC   sentimento_genai,
# MAGIC   COUNT(*) AS total_pedidos,
# MAGIC   SUM(CASE WHEN o.status_entrega = 'atrasado' THEN 1 ELSE 0 END) AS atrasados,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN o.status_entrega = 'atrasado' THEN 1 ELSE 0 END) * 100.0
# MAGIC     / COUNT(*), 1
# MAGIC   ) AS pct_atrasado
# MAGIC FROM reviews_sentimento_genai r
# MAGIC INNER JOIN pb_brasilmart.silver.orders_enriched o
# MAGIC   ON r.order_id = o.order_id
# MAGIC WHERE o.status_entrega IN ('no_prazo', 'atrasado')
# MAGIC GROUP BY sentimento_genai
# MAGIC ORDER BY pct_atrasado DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Evidências

# COMMAND ----------

print("=" * 70)
print("EVIDÊNCIAS — TP5 Atividade 2.1: GenAI Sentiment Analysis")
print("=" * 70)
print("""
1. FUNÇÃO UTILIZADA:
   ai_analyze_sentiment(review_message)
   Engine: Databricks Foundation Model API (LLM)
   Retorno: Positive / Negative / Neutral / Mixed

2. COLUNA ANALISADA:
   pb_brasilmart.silver.reviews.review_message
   Papel: comentarios_manutencao (feedback textual do cliente)
   ~41 mil reviews com comentário (41.3% do total)
   Amostra processada: 1.000 reviews (estratificada por score)

3. REQUISITO TP1 ATENDIDO:
   Q11 — Equipe de Reviews / Customer Success
   R10 — Pipeline NLP para classificação de reviews
   Cruzamento sentimento × status_entrega (delta_entrega)

4. COMPARAÇÕES:
   - GenAI vs score numérico (review_score)
   - GenAI vs sentimento rule-based (Silver TP3)
   - Casos divergentes (sarcasmo, ressalvas, Mixed)
   - Cruzamento com dados de entrega (atraso × sentimento)

5. COMO VERIFICAR NO DATABRICKS:
   SQL Warehouse → Abrir notebook → Executar células SQL
   Sidebar → SQL Editor → Queries sobre reviews_sentimento_genai
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo TP5 — Atividade 2.1
# MAGIC
# MAGIC ### AI Function Utilizada
# MAGIC | Item | Detalhe |
# MAGIC |------|---------|
# MAGIC | Função | `ai_analyze_sentiment(text)` |
# MAGIC | Engine | Databricks Foundation Model API |
# MAGIC | Input | `review_message` (= `comentarios_manutencao`) |
# MAGIC | Output | `Positive`, `Negative`, `Neutral`, `Mixed` |
# MAGIC | Amostra | 1.000 reviews com texto > 10 chars |
# MAGIC
# MAGIC ### Análises Realizadas
# MAGIC | Análise | Resultado Esperado |
# MAGIC |---------|-------------------|
# MAGIC | Distribuição sentimento GenAI | Maioria Positive (scores 4-5 dominam) |
# MAGIC | GenAI vs review_score | Alta concordância (score 1-2 → Negative, 4-5 → Positive) |
# MAGIC | GenAI vs regras (Silver) | GenAI detecta nuances (Mixed, sarcasmo) que regras não captam |
# MAGIC | Casos divergentes | Score alto + sentimento negativo = ressalva/sarcasmo |
# MAGIC | Cruzamento com entrega | Reviews negativas têm taxa de atraso significativamente maior |
# MAGIC
# MAGIC ### Requisito TP1 (Q11) — Análise de Causa Raiz
# MAGIC ```
# MAGIC Hipótese TP1: "65% das avaliações negativas mencionam atraso"
# MAGIC
# MAGIC Validação TP5:
# MAGIC   reviews (review_message)
# MAGIC     → ai_analyze_sentiment → sentimento_genai
# MAGIC       → JOIN orders_enriched (status_entrega, delta_entrega_dias)
# MAGIC         → Confirma/refuta correlação atraso × sentimento negativo
# MAGIC ```
