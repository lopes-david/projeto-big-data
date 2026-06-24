# Databricks notebook source
# MAGIC %md
# MAGIC # TP5 — Atividade 4.1: Dashboard QuickSight (BI) conectado ao Redshift
# MAGIC
# MAGIC ## Objetivo
# MAGIC Dashboard no Amazon QuickSight conectado ao Redshift com:
# MAGIC - **4.1.1 KPIs**: métricas dos dbt Marts (requisitos TP1)
# MAGIC - **4.1.2 Insights**: análises derivadas dos requisitos do TP1
# MAGIC - **4.1.3 Inteligência (Feedback Loop)**: alertas de probabilidade de falha (predições ML)
# MAGIC
# MAGIC ## Conexão
# MAGIC | Item | Valor |
# MAGIC |------|-------|
# MAGIC | **Data Source** | Amazon Redshift Serverless |
# MAGIC | **Host** | `default-workgroup.234828142988.sa-east-1.redshift-serverless.amazonaws.com:5439` |
# MAGIC | **Database** | `dev` |
# MAGIC | **Schema** | `pb_gold` (dbt marts) |
# MAGIC | **DataSets** | 5 (um por aba do dashboard) |
# MAGIC
# MAGIC ## Estrutura do Dashboard
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────────┐
# MAGIC │  Dashboard BrasilMart — Visão 360° do Cliente                       │
# MAGIC ├──────────────────────────────────────────────────────────────────────┤
# MAGIC │  [Aba 1: KPIs Executivos] [Aba 2: Clientes] [Aba 3: Vendedores]   │
# MAGIC │  [Aba 4: Produtos]       [Aba 5: Inteligência ML]                  │
# MAGIC └──────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Aba 1 — KPIs Executivos (fato_vendas_diarias)
# MAGIC **Requisito TP1:** R4 (Q5 — Equipe de BI): "Pipeline diário de GMV por categoria e estado"
# MAGIC
# MAGIC ### Layout
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │  KPI 1          KPI 2          KPI 3          KPI 4            │
# MAGIC │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
# MAGIC │  │  GMV     │  │  Pedidos │  │  Ticket  │  │  Frete   │      │
# MAGIC │  │  Total   │  │  Total   │  │  Médio   │  │  Total   │      │
# MAGIC │  │ R$13.6M  │  │  96.478  │  │  R$141   │  │  R$2.3M  │      │
# MAGIC │  └──────────┘  └──────────┘  └──────────┘  └──────────┘      │
# MAGIC │                                                               │
# MAGIC │  ┌─────────────────────────────────────────────────────────┐  │
# MAGIC │  │  Gráfico de Linha: GMV Diário (série temporal)          │  │
# MAGIC │  │  eixo X: data_venda | eixo Y: gmv                      │  │
# MAGIC │  └─────────────────────────────────────────────────────────┘  │
# MAGIC │                                                               │
# MAGIC │  ┌──────────────────────────┐ ┌────────────────────────────┐  │
# MAGIC │  │  Barras: Pedidos/Dia     │ │  Linha: Ticket Médio/Dia   │  │
# MAGIC │  │  (total_pedidos)         │ │  (ticket_medio)            │  │
# MAGIC │  └──────────────────────────┘ └────────────────────────────┘  │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Queries — Aba 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KPI Cards: totais consolidados
# MAGIC SELECT
# MAGIC   SUM(gmv) AS gmv_total,
# MAGIC   SUM(total_pedidos) AS pedidos_total,
# MAGIC   ROUND(SUM(gmv) / NULLIF(SUM(total_pedidos), 0), 2) AS ticket_medio_geral,
# MAGIC   SUM(total_frete) AS frete_total,
# MAGIC   SUM(total_clientes) AS clientes_total,
# MAGIC   COUNT(DISTINCT data_venda) AS dias_operacao
# MAGIC FROM pb_gold.fato_vendas_diarias

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Série temporal: GMV diário + média móvel 7 dias
# MAGIC SELECT
# MAGIC   data_venda,
# MAGIC   gmv,
# MAGIC   total_pedidos,
# MAGIC   ticket_medio,
# MAGIC   total_frete,
# MAGIC   AVG(gmv) OVER (ORDER BY data_venda ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS gmv_media_7d,
# MAGIC   AVG(total_pedidos) OVER (ORDER BY data_venda ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS pedidos_media_7d
# MAGIC FROM pb_gold.fato_vendas_diarias
# MAGIC ORDER BY data_venda

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Aba 2 — Clientes RFM (dim_clientes_rfm)
# MAGIC **Requisito TP1:** R1 (Q1 — CEO, Q2 — Marketing): "Segmentação RFM consolidada na Gold"
# MAGIC
# MAGIC ### Layout
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │  KPI 1          KPI 2          KPI 3          KPI 4            │
# MAGIC │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
# MAGIC │  │ Clientes │  │Champions │  │ At Risk  │  │   Lost   │      │
# MAGIC │  │  96.096  │  │   0.2%   │  │   1.2%   │  │  22.5%   │      │
# MAGIC │  └──────────┘  └──────────┘  └──────────┘  └──────────┘      │
# MAGIC │                                                               │
# MAGIC │  ┌──────────────────────────┐ ┌────────────────────────────┐  │
# MAGIC │  │  Donut: Distribuição     │ │  Barras: Monetary Médio    │  │
# MAGIC │  │  por Segmento RFM        │ │  por Segmento              │  │
# MAGIC │  └──────────────────────────┘ └────────────────────────────┘  │
# MAGIC │                                                               │
# MAGIC │  ┌─────────────────────────────────────────────────────────┐  │
# MAGIC │  │  Mapa: Clientes por Estado (heatmap geográfico)         │  │
# MAGIC │  │  cor = total_clientes | tooltip = monetary, frequency   │  │
# MAGIC │  └─────────────────────────────────────────────────────────┘  │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Queries — Aba 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribuição por segmento RFM
# MAGIC SELECT
# MAGIC   rfm_segment,
# MAGIC   COUNT(*) AS total_clientes,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct,
# MAGIC   ROUND(AVG(monetary), 2) AS monetary_medio,
# MAGIC   ROUND(AVG(frequency), 2) AS frequency_media,
# MAGIC   ROUND(AVG(recency_days), 0) AS recency_media,
# MAGIC   ROUND(AVG(avg_ticket), 2) AS ticket_medio
# MAGIC FROM pb_gold.dim_clientes_rfm
# MAGIC GROUP BY rfm_segment
# MAGIC ORDER BY total_clientes DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clientes e monetary por estado (para mapa geográfico — TP1 Q9)
# MAGIC SELECT
# MAGIC   customer_state,
# MAGIC   COUNT(*) AS total_clientes,
# MAGIC   ROUND(SUM(monetary), 2) AS monetary_total,
# MAGIC   ROUND(AVG(monetary), 2) AS monetary_medio,
# MAGIC   ROUND(AVG(frequency), 2) AS frequency_media
# MAGIC FROM pb_gold.dim_clientes_rfm
# MAGIC GROUP BY customer_state
# MAGIC ORDER BY total_clientes DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insight: Clientes At Risk e Lost com alto valor (ação de retenção)
# MAGIC SELECT
# MAGIC   rfm_segment,
# MAGIC   customer_state,
# MAGIC   COUNT(*) AS total,
# MAGIC   ROUND(AVG(monetary), 2) AS monetary_medio,
# MAGIC   ROUND(AVG(recency_days), 0) AS recency_media
# MAGIC FROM pb_gold.dim_clientes_rfm
# MAGIC WHERE rfm_segment IN ('At Risk', 'Lost')
# MAGIC   AND monetary > 200
# MAGIC GROUP BY rfm_segment, customer_state
# MAGIC ORDER BY monetary_medio DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Aba 3 — Vendedores (dim_sellers_score)
# MAGIC **Requisito TP1:** R3 (Q4 — Diretor Marketplace): "Seller Score composto"
# MAGIC
# MAGIC ### Layout
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │  KPI 1          KPI 2          KPI 3          KPI 4            │
# MAGIC │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
# MAGIC │  │ Sellers  │  │Excelente │  │ Crítico  │  │Cancel %  │      │
# MAGIC │  │  3.095   │  │   12%    │  │   25%    │  │  0.6%    │      │
# MAGIC │  └──────────┘  └──────────┘  └──────────┘  └──────────┘      │
# MAGIC │                                                               │
# MAGIC │  ┌──────────────────────────┐ ┌────────────────────────────┐  │
# MAGIC │  │  Donut: Distribuição     │ │  Scatter: Score vs Volume  │  │
# MAGIC │  │  por Tier                │ │  (seller_score × orders)   │  │
# MAGIC │  └──────────────────────────┘ └────────────────────────────┘  │
# MAGIC │                                                               │
# MAGIC │  ┌─────────────────────────────────────────────────────────┐  │
# MAGIC │  │  Tabela: Top 10 vendedores críticos (score + reviews)   │  │
# MAGIC │  └─────────────────────────────────────────────────────────┘  │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Queries — Aba 3

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribuição por tier
# MAGIC SELECT
# MAGIC   seller_tier,
# MAGIC   COUNT(*) AS total_sellers,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct,
# MAGIC   ROUND(AVG(seller_score), 2) AS score_medio,
# MAGIC   ROUND(AVG(avg_review_score), 2) AS review_medio,
# MAGIC   ROUND(AVG(on_time_rate), 1) AS on_time_medio,
# MAGIC   ROUND(AVG(cancel_rate), 2) AS cancel_medio,
# MAGIC   SUM(total_orders) AS pedidos_total
# MAGIC FROM pb_gold.dim_sellers_score
# MAGIC GROUP BY seller_tier
# MAGIC ORDER BY score_medio DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insight: Vendedores críticos — alto volume + baixa avaliação (TP1 Q4)
# MAGIC SELECT
# MAGIC   seller_id,
# MAGIC   seller_state,
# MAGIC   total_orders,
# MAGIC   ROUND(seller_score, 2) AS seller_score,
# MAGIC   seller_tier,
# MAGIC   ROUND(avg_review_score, 2) AS avg_review,
# MAGIC   negative_reviews,
# MAGIC   ROUND(on_time_rate, 1) AS on_time_pct,
# MAGIC   ROUND(cancel_rate, 2) AS cancel_pct
# MAGIC FROM pb_gold.dim_sellers_score
# MAGIC WHERE seller_tier = 'Critico'
# MAGIC   AND total_orders >= 10
# MAGIC ORDER BY seller_score ASC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Scatter plot data: seller_score vs total_orders, cor = tier
# MAGIC SELECT
# MAGIC   seller_id,
# MAGIC   seller_state,
# MAGIC   seller_score,
# MAGIC   total_orders,
# MAGIC   avg_review_score,
# MAGIC   seller_tier
# MAGIC FROM pb_gold.dim_sellers_score
# MAGIC WHERE total_orders >= 5

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Aba 4 — Produtos (dim_produtos_performance)
# MAGIC **Requisito TP1:** R7 (Q8 — Diretor Catálogo): "Catálogo com performance analytics"
# MAGIC
# MAGIC ### Layout
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │  KPI 1          KPI 2          KPI 3          KPI 4            │
# MAGIC │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
# MAGIC │  │Produtos  │  │  Ativos  │  │ Inativos │  │Review Neg│      │
# MAGIC │  │  32.951  │  │   15%    │  │   60%    │  │  12.3%   │      │
# MAGIC │  └──────────┘  └──────────┘  └──────────┘  └──────────┘      │
# MAGIC │                                                               │
# MAGIC │  ┌──────────────────────────┐ ┌────────────────────────────┐  │
# MAGIC │  │  Donut: Distribuição     │ │  Barras: Top 10 Categorias │  │
# MAGIC │  │  por Status              │ │  por Revenue Total         │  │
# MAGIC │  └──────────────────────────┘ └────────────────────────────┘  │
# MAGIC │                                                               │
# MAGIC │  ┌─────────────────────────────────────────────────────────┐  │
# MAGIC │  │  Tabela: Produtos inativos com alto revenue histórico   │  │
# MAGIC │  │  (candidatos a reativação/incentivo ao vendedor)        │  │
# MAGIC │  └─────────────────────────────────────────────────────────┘  │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Queries — Aba 4

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribuição por status de atividade
# MAGIC SELECT
# MAGIC   product_status,
# MAGIC   COUNT(*) AS total_produtos,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct,
# MAGIC   ROUND(SUM(total_revenue), 2) AS revenue_total,
# MAGIC   ROUND(AVG(avg_review_score), 2) AS review_medio,
# MAGIC   ROUND(AVG(negative_review_rate), 1) AS neg_review_pct_media
# MAGIC FROM pb_gold.dim_produtos_performance
# MAGIC GROUP BY product_status
# MAGIC ORDER BY total_produtos DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 categorias por revenue (TP1 Q5 — GMV por categoria)
# MAGIC SELECT
# MAGIC   product_category,
# MAGIC   COUNT(*) AS total_produtos,
# MAGIC   SUM(total_orders) AS pedidos_total,
# MAGIC   ROUND(SUM(total_revenue), 2) AS revenue_total,
# MAGIC   ROUND(AVG(avg_review_score), 2) AS review_medio
# MAGIC FROM pb_gold.dim_produtos_performance
# MAGIC GROUP BY product_category
# MAGIC ORDER BY revenue_total DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insight: produtos inativos com alto valor histórico (TP1 Q8)
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   product_category,
# MAGIC   total_orders,
# MAGIC   total_revenue,
# MAGIC   avg_review_score,
# MAGIC   days_since_last_sale,
# MAGIC   product_status
# MAGIC FROM pb_gold.dim_produtos_performance
# MAGIC WHERE product_status = 'Inativo'
# MAGIC   AND total_revenue > 500
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Aba 5 — Inteligência ML / Feedback Loop (predicoes_databricks_ml)
# MAGIC **Requisito TP1:** R2 (Q3 — Diretor Operações): "Modelo preditivo de atraso"
# MAGIC
# MAGIC ### Layout
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │  KPI 1          KPI 2          KPI 3          KPI 4            │
# MAGIC │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
# MAGIC │  │ Pedidos  │  │Risco Alto│  │Prob Média│  │ Accuracy │      │
# MAGIC │  │Scored    │  │  15.2%   │  │  0.123   │  │  93.5%   │      │
# MAGIC │  └──────────┘  └──────────┘  └──────────┘  └──────────┘      │
# MAGIC │                                                               │
# MAGIC │  ┌─────────────────────────────────────────────────────────┐  │
# MAGIC │  │  *** GRÁFICO PRINCIPAL: Alerta de Probabilidade ***     │  │
# MAGIC │  │  Histograma: distribuição de probabilidade_falha        │  │
# MAGIC │  │  com linhas de threshold (0.3 = médio, 0.6 = alto)     │  │
# MAGIC │  │                                                         │  │
# MAGIC │  │  prob │                                                  │  │
# MAGIC │  │  0.9  │                                          █      │  │
# MAGIC │  │  0.8  │                                        ███      │  │
# MAGIC │  │  0.7  │                                      █████      │  │
# MAGIC │  │  0.6  │- - - - - - - - ALTO - - - - - - - -██████      │  │
# MAGIC │  │  0.5  │                                   ████████      │  │
# MAGIC │  │  0.4  │                                 ██████████      │  │
# MAGIC │  │  0.3  │- - - - - - - MÉDIO - - - - - ████████████      │  │
# MAGIC │  │  0.2  │                            ██████████████████   │  │
# MAGIC │  │  0.1  │  ██████████████████████████████████████████████ │  │
# MAGIC │  │       └──────────────────────────────────────────────── │  │
# MAGIC │  │         pedidos (ordenados por probabilidade)           │  │
# MAGIC │  └─────────────────────────────────────────────────────────┘  │
# MAGIC │                                                               │
# MAGIC │  ┌──────────────────────────┐ ┌────────────────────────────┐  │
# MAGIC │  │  Donut: Distribuição     │ │  Barras: Risco por Estado  │  │
# MAGIC │  │  por Faixa de Risco      │ │  (customer_state)          │  │
# MAGIC │  └──────────────────────────┘ └────────────────────────────┘  │
# MAGIC │                                                               │
# MAGIC │  ┌─────────────────────────────────────────────────────────┐  │
# MAGIC │  │  Tabela: Top 20 pedidos com maior probabilidade_falha   │  │
# MAGIC │  │  (alerta operacional para ação proativa)                │  │
# MAGIC │  └─────────────────────────────────────────────────────────┘  │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Queries — Aba 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KPI Cards: resumo das predições
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_scored,
# MAGIC   SUM(CASE WHEN risco_atraso = 'alto' THEN 1 ELSE 0 END) AS risco_alto,
# MAGIC   ROUND(SUM(CASE WHEN risco_atraso = 'alto' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_risco_alto,
# MAGIC   ROUND(AVG(probabilidade_falha), 4) AS prob_media,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN predicao_atraso = label THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
# MAGIC   ) AS accuracy_pct,
# MAGIC   modelo_versao
# MAGIC FROM pb_gold.predicoes_databricks_ml
# MAGIC GROUP BY modelo_versao

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GRÁFICO PRINCIPAL: distribuição de probabilidade_falha (histograma)
# MAGIC -- Cada bucket de 0.05 mostra a contagem de pedidos naquela faixa
# MAGIC SELECT
# MAGIC   ROUND(FLOOR(probabilidade_falha * 20) / 20.0, 2) AS faixa_prob,
# MAGIC   COUNT(*) AS total_pedidos,
# MAGIC   SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END) AS realmente_atrasados,
# MAGIC   ROUND(AVG(total_pago), 2) AS ticket_medio
# MAGIC FROM pb_gold.predicoes_databricks_ml
# MAGIC GROUP BY faixa_prob
# MAGIC ORDER BY faixa_prob

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribuição por faixa de risco
# MAGIC SELECT
# MAGIC   risco_atraso,
# MAGIC   COUNT(*) AS total_pedidos,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct,
# MAGIC   SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END) AS realmente_atrasados,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
# MAGIC   ) AS taxa_atraso_real_pct,
# MAGIC   ROUND(AVG(probabilidade_falha), 4) AS prob_media,
# MAGIC   ROUND(SUM(total_pago), 2) AS valor_total_em_risco
# MAGIC FROM pb_gold.predicoes_databricks_ml
# MAGIC GROUP BY risco_atraso
# MAGIC ORDER BY prob_media DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Risco de atraso por estado do cliente (barras horizontais)
# MAGIC SELECT
# MAGIC   customer_state,
# MAGIC   COUNT(*) AS total_pedidos,
# MAGIC   SUM(CASE WHEN risco_atraso = 'alto' THEN 1 ELSE 0 END) AS risco_alto,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN risco_atraso = 'alto' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
# MAGIC   ) AS pct_risco_alto,
# MAGIC   ROUND(AVG(probabilidade_falha), 4) AS prob_media
# MAGIC FROM pb_gold.predicoes_databricks_ml
# MAGIC GROUP BY customer_state
# MAGIC HAVING COUNT(*) >= 50
# MAGIC ORDER BY pct_risco_alto DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ALERTA: Top 20 pedidos com maior probabilidade de falha
# MAGIC -- (tabela operacional para ação proativa — TP1 Q3)
# MAGIC SELECT
# MAGIC   p.order_id,
# MAGIC   p.customer_state,
# MAGIC   p.seller_state,
# MAGIC   ROUND(p.total_pago, 2) AS total_pago,
# MAGIC   p.qtd_itens,
# MAGIC   ROUND(p.peso_medio_kg, 2) AS peso_kg,
# MAGIC   ROUND(p.probabilidade_falha, 4) AS probabilidade_falha,
# MAGIC   p.risco_atraso,
# MAGIC   p.predicao_atraso,
# MAGIC   p.label AS atraso_real
# MAGIC FROM pb_gold.predicoes_databricks_ml p
# MAGIC ORDER BY p.probabilidade_falha DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insight cruzado: risco ML × segmento RFM do cliente
# MAGIC SELECT
# MAGIC   c.rfm_segment,
# MAGIC   p.risco_atraso,
# MAGIC   COUNT(*) AS total_pedidos,
# MAGIC   ROUND(AVG(p.probabilidade_falha), 4) AS prob_media,
# MAGIC   ROUND(AVG(p.total_pago), 2) AS ticket_medio
# MAGIC FROM pb_gold.predicoes_databricks_ml p
# MAGIC LEFT JOIN pb_gold.dim_clientes_rfm c
# MAGIC   ON p.customer_id = c.customer_unique_id
# MAGIC GROUP BY c.rfm_segment, p.risco_atraso
# MAGIC ORDER BY c.rfm_segment, p.risco_atraso

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Configuração no QuickSight

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Script de Setup
# MAGIC
# MAGIC O script `infra/aws/setup_quicksight.sh` automatiza:
# MAGIC 1. **Data Source**: conexão Redshift Serverless (`pb-brasilmart-redshift-ds`)
# MAGIC 2. **DataSets** (5):
# MAGIC    - `ds-vendas-diarias` → `pb_gold.fato_vendas_diarias`
# MAGIC    - `ds-clientes-rfm` → `pb_gold.dim_clientes_rfm`
# MAGIC    - `ds-sellers-score` → `pb_gold.dim_sellers_score`
# MAGIC    - `ds-produtos-perf` → `pb_gold.dim_produtos_performance`
# MAGIC    - `ds-predicoes-ml` → `pb_gold.predicoes_databricks_ml`
# MAGIC
# MAGIC ### 5.2 Criação Manual das Visualizações
# MAGIC
# MAGIC ```
# MAGIC QuickSight Console → Analyses → New Analysis
# MAGIC   → Selecionar cada DataSet
# MAGIC   → Criar as visualizações conforme layouts acima
# MAGIC   → Publish como Dashboard
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Mapeamento Requisitos TP1 → Visualizações
# MAGIC
# MAGIC | Requisito TP1 | Stakeholder | Visualização QuickSight | DataSet | Aba |
# MAGIC |---------------|------------|------------------------|---------|-----|
# MAGIC | R1 — Segmentação RFM | CEO, Marketing (Q1, Q2) | Donut segmentos + KPI cards + Mapa por estado | `ds-clientes-rfm` | 2 |
# MAGIC | R2 — Delta entrega + predição atraso | Operações (Q3) | Histograma probabilidade_falha + Tabela alertas | `ds-predicoes-ml` | 5 |
# MAGIC | R3 — Seller Score composto | Marketplace (Q4) | Donut tiers + Scatter score×volume + Tabela críticos | `ds-sellers-score` | 3 |
# MAGIC | R4 — GMV diário por categoria | BI (Q5) | Linha GMV + Barras pedidos + KPI ticket médio | `ds-vendas-diarias` | 1 |
# MAGIC | R7 — Performance de produtos | Catálogo (Q8) | Donut status + Barras categorias + Tabela inativos | `ds-produtos-perf` | 4 |
# MAGIC | R8 — Análise geoespacial | Marketing (Q9) | Mapa clientes por estado (Aba 2) | `ds-clientes-rfm` | 2 |
# MAGIC | Feedback Loop ML | Operações (Q3) | **Gráfico de alerta de probabilidade de falha** | `ds-predicoes-ml` | **5** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Evidências

# COMMAND ----------

print("=" * 70)
print("EVIDÊNCIAS — TP5 Atividade 4.1: Dashboard QuickSight")
print("=" * 70)
print("""
1. CONEXÃO:
   Data Source: Amazon Redshift Serverless
   Host: default-workgroup.234828142988.sa-east-1.redshift-serverless.amazonaws.com
   Database: dev | Schema: pb_gold
   Modo: DIRECT_QUERY (queries em tempo real)

2. DATASETS (5):
   - ds-vendas-diarias  → fato_vendas_diarias (KPIs executivos)
   - ds-clientes-rfm    → dim_clientes_rfm (segmentação clientes)
   - ds-sellers-score   → dim_sellers_score (score vendedores)
   - ds-produtos-perf   → dim_produtos_performance (catálogo)
   - ds-predicoes-ml    → predicoes_databricks_ml (Feedback Loop)

3. DASHBOARD — 5 ABAS:
   Aba 1: KPIs Executivos (GMV, pedidos, ticket médio, frete)
   Aba 2: Clientes RFM (segmentos, mapa geográfico, at-risk)
   Aba 3: Vendedores (tiers, score, críticos)
   Aba 4: Produtos (status, categorias, inativos)
   Aba 5: Inteligência ML (probabilidade_falha, alertas, risco)

4. REQUISITOS TP1 ATENDIDOS:
   R1 (Q1, Q2) — Segmentação RFM ✓
   R2 (Q3)     — Predição de atraso + alerta ✓
   R3 (Q4)     — Seller Score composto ✓
   R4 (Q5)     — GMV diário ✓
   R7 (Q8)     — Performance de produtos ✓
   R8 (Q9)     — Mapa geográfico ✓

5. FEEDBACK LOOP (4.1.3):
   Gráfico principal: Histograma de probabilidade_falha
   Thresholds: 0.3 (médio) e 0.6 (alto)
   Tabela de alerta: Top 20 pedidos com maior risco
   Cruzamento: risco ML × segmento RFM do cliente

6. COMO VERIFICAR:
   AWS Console → QuickSight → Dashboards → BrasilMart
   Script: infra/aws/setup_quicksight.sh
   Queries: neste notebook (todas executáveis no Redshift)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo TP5 — Atividade 4.1
# MAGIC
# MAGIC ### Arquitetura do Dashboard
# MAGIC ```
# MAGIC ┌────────────────┐    ┌──────────────────┐    ┌──────────────────┐
# MAGIC │ Redshift        │    │  QuickSight       │    │  Usuários        │
# MAGIC │ pb_gold.*       │───→│  5 DataSets       │───→│  CEO, Marketing  │
# MAGIC │                 │    │  (DIRECT_QUERY)    │    │  Operações, BI   │
# MAGIC │ • fato_vendas   │    │                    │    │  Marketplace     │
# MAGIC │ • dim_clientes  │    │  Dashboard         │    │                  │
# MAGIC │ • dim_sellers   │    │  5 abas + 15+      │    │  Decisões        │
# MAGIC │ • dim_produtos  │    │  visualizações     │    │  data-driven     │
# MAGIC │ • predicoes_ml  │    │                    │    │                  │
# MAGIC └────────────────┘    └──────────────────┘    └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Fluxo Completo TP5 (End-to-End)
# MAGIC ```
# MAGIC Redshift Gold (dbt)
# MAGIC   → features_ml (TP5 1.1)
# MAGIC     → Databricks ML (TP5 1.2)
# MAGIC       → predicoes_databricks_ml (TP5 1.3)
# MAGIC         → QuickSight Dashboard (TP5 4.1)
# MAGIC           → Alerta de probabilidade de falha (TP5 4.1.3)
# MAGIC             → Ação operacional proativa
# MAGIC ```
