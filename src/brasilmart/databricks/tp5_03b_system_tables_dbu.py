# Databricks notebook source
# MAGIC %md
# MAGIC # TP5 — Atividade 3.2: System Tables — Consumo de DBUs
# MAGIC
# MAGIC ## Objetivo
# MAGIC Consultar as **System Tables** do Databricks para analisar o consumo de
# MAGIC DBUs (Databricks Units) das cargas de trabalho de ML e DLT do projeto,
# MAGIC medindo a potência de processamento computacional consumida.
# MAGIC
# MAGIC ## System Tables Consultadas
# MAGIC | System Table | O que contém |
# MAGIC |-------------|-------------|
# MAGIC | `system.billing.usage` | Consumo de DBUs por SKU, workspace e período |
# MAGIC | `system.billing.list_prices` | Preço por DBU de cada SKU |
# MAGIC | `system.compute.clusters` | Inventário de clusters e compute |
# MAGIC | `system.lakeflow.pipeline_event_log` | Eventos dos pipelines DLT |
# MAGIC
# MAGIC ## Contexto de Infraestrutura
# MAGIC | Recurso | Tipo Compute | SKU Esperado |
# MAGIC |---------|-------------|-------------|
# MAGIC | Notebooks (TP1–TP5) | Serverless Compute | `ENTERPRISE_SERVERLESS_COMPUTE` |
# MAGIC | SQL Warehouse | Serverless SQL | `ENTERPRISE_SERVERLESS_SQL` |
# MAGIC | DLT Silver (11 tabelas) | Serverless DLT | `ENTERPRISE_SERVERLESS_DLT` |
# MAGIC | DLT CDC (4 tabelas) | Serverless DLT | `ENTERPRISE_SERVERLESS_DLT` |
# MAGIC | Model Serving | Serverless Serving | `ENTERPRISE_MODEL_SERVING` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Consumo Total de DBUs por SKU

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consumo total de DBUs agrupado por SKU (tipo de workload)
# MAGIC SELECT
# MAGIC   sku_name,
# MAGIC   billing_origin_product,
# MAGIC   SUM(usage_quantity) AS total_dbus,
# MAGIC   COUNT(DISTINCT usage_date) AS dias_com_uso,
# MAGIC   MIN(usage_date) AS primeiro_uso,
# MAGIC   MAX(usage_date) AS ultimo_uso
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= '2026-06-01'
# MAGIC GROUP BY sku_name, billing_origin_product
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Consumo de DBUs — DLT Pipelines

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consumo de DBUs pelas cargas DLT (Delta Live Tables)
# MAGIC SELECT
# MAGIC   sku_name,
# MAGIC   usage_metadata.cluster_id,
# MAGIC   usage_metadata.job_id,
# MAGIC   SUM(usage_quantity) AS total_dbus,
# MAGIC   COUNT(*) AS registros_billing,
# MAGIC   MIN(usage_date) AS primeiro_uso,
# MAGIC   MAX(usage_date) AS ultimo_uso
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= '2026-06-01'
# MAGIC   AND (
# MAGIC     LOWER(sku_name) LIKE '%dlt%'
# MAGIC     OR LOWER(sku_name) LIKE '%delta_live%'
# MAGIC     OR LOWER(sku_name) LIKE '%pipelines%'
# MAGIC     OR LOWER(billing_origin_product) LIKE '%dlt%'
# MAGIC     OR LOWER(billing_origin_product) LIKE '%pipelines%'
# MAGIC   )
# MAGIC GROUP BY sku_name, usage_metadata.cluster_id, usage_metadata.job_id
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consumo DLT diário (série temporal)
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   sku_name,
# MAGIC   SUM(usage_quantity) AS dbus_dia
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= '2026-06-01'
# MAGIC   AND (
# MAGIC     LOWER(sku_name) LIKE '%dlt%'
# MAGIC     OR LOWER(sku_name) LIKE '%pipelines%'
# MAGIC     OR LOWER(billing_origin_product) LIKE '%dlt%'
# MAGIC   )
# MAGIC GROUP BY usage_date, sku_name
# MAGIC ORDER BY usage_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Consumo de DBUs — ML / Model Serving

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consumo de DBUs por cargas ML (treinamento + serving)
# MAGIC SELECT
# MAGIC   sku_name,
# MAGIC   billing_origin_product,
# MAGIC   usage_metadata.cluster_id,
# MAGIC   SUM(usage_quantity) AS total_dbus,
# MAGIC   COUNT(*) AS registros_billing,
# MAGIC   MIN(usage_date) AS primeiro_uso,
# MAGIC   MAX(usage_date) AS ultimo_uso
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= '2026-06-01'
# MAGIC   AND (
# MAGIC     LOWER(sku_name) LIKE '%serving%'
# MAGIC     OR LOWER(sku_name) LIKE '%model%'
# MAGIC     OR LOWER(sku_name) LIKE '%inference%'
# MAGIC     OR LOWER(sku_name) LIKE '%ml%'
# MAGIC     OR LOWER(billing_origin_product) LIKE '%serving%'
# MAGIC     OR LOWER(billing_origin_product) LIKE '%model%'
# MAGIC   )
# MAGIC GROUP BY sku_name, billing_origin_product, usage_metadata.cluster_id
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consumo ML + Serving diário
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   sku_name,
# MAGIC   SUM(usage_quantity) AS dbus_dia
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= '2026-06-01'
# MAGIC   AND (
# MAGIC     LOWER(sku_name) LIKE '%serving%'
# MAGIC     OR LOWER(sku_name) LIKE '%model%'
# MAGIC     OR LOWER(sku_name) LIKE '%inference%'
# MAGIC     OR LOWER(billing_origin_product) LIKE '%serving%'
# MAGIC   )
# MAGIC GROUP BY usage_date, sku_name
# MAGIC ORDER BY usage_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Consumo de DBUs — Serverless Compute (Notebooks)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consumo de Serverless Compute (notebooks interativos)
# MAGIC -- Inclui execução dos notebooks de ML (tp4_05, tp4_06) e Feedback Loop (tp5_01)
# MAGIC SELECT
# MAGIC   sku_name,
# MAGIC   usage_metadata.cluster_id,
# MAGIC   SUM(usage_quantity) AS total_dbus,
# MAGIC   COUNT(*) AS registros_billing,
# MAGIC   MIN(usage_date) AS primeiro_uso,
# MAGIC   MAX(usage_date) AS ultimo_uso
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= '2026-06-01'
# MAGIC   AND (
# MAGIC     LOWER(sku_name) LIKE '%serverless%compute%'
# MAGIC     OR LOWER(sku_name) LIKE '%interactive%'
# MAGIC     OR LOWER(sku_name) LIKE '%notebook%'
# MAGIC   )
# MAGIC   AND LOWER(sku_name) NOT LIKE '%sql%'
# MAGIC   AND LOWER(sku_name) NOT LIKE '%dlt%'
# MAGIC GROUP BY sku_name, usage_metadata.cluster_id
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Consumo de DBUs — SQL Warehouse

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consumo do SQL Warehouse (DBSQL — queries GenAI, monitoring, ad-hoc)
# MAGIC SELECT
# MAGIC   sku_name,
# MAGIC   usage_metadata.warehouse_id,
# MAGIC   SUM(usage_quantity) AS total_dbus,
# MAGIC   COUNT(*) AS registros_billing,
# MAGIC   MIN(usage_date) AS primeiro_uso,
# MAGIC   MAX(usage_date) AS ultimo_uso
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= '2026-06-01'
# MAGIC   AND (
# MAGIC     LOWER(sku_name) LIKE '%sql%'
# MAGIC     OR LOWER(sku_name) LIKE '%warehouse%'
# MAGIC   )
# MAGIC GROUP BY sku_name, usage_metadata.warehouse_id
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Visão Consolidada — DBUs por Dia e Categoria

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Série temporal de consumo por categoria de workload
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   CASE
# MAGIC     WHEN LOWER(sku_name) LIKE '%dlt%' OR LOWER(sku_name) LIKE '%pipelines%'
# MAGIC       THEN 'DLT Pipelines'
# MAGIC     WHEN LOWER(sku_name) LIKE '%serving%' OR LOWER(sku_name) LIKE '%model%'
# MAGIC       THEN 'Model Serving'
# MAGIC     WHEN LOWER(sku_name) LIKE '%sql%' OR LOWER(sku_name) LIKE '%warehouse%'
# MAGIC       THEN 'SQL Warehouse'
# MAGIC     WHEN LOWER(sku_name) LIKE '%serverless%compute%' OR LOWER(sku_name) LIKE '%interactive%'
# MAGIC       THEN 'Serverless Compute'
# MAGIC     ELSE 'Outros'
# MAGIC   END AS categoria_workload,
# MAGIC   SUM(usage_quantity) AS dbus_dia
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= '2026-06-01'
# MAGIC GROUP BY usage_date, categoria_workload
# MAGIC ORDER BY usage_date, categoria_workload

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Resumo consolidado: total de DBUs por categoria
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN LOWER(sku_name) LIKE '%dlt%' OR LOWER(sku_name) LIKE '%pipelines%'
# MAGIC       THEN 'DLT Pipelines'
# MAGIC     WHEN LOWER(sku_name) LIKE '%serving%' OR LOWER(sku_name) LIKE '%model%'
# MAGIC       THEN 'Model Serving'
# MAGIC     WHEN LOWER(sku_name) LIKE '%sql%' OR LOWER(sku_name) LIKE '%warehouse%'
# MAGIC       THEN 'SQL Warehouse'
# MAGIC     WHEN LOWER(sku_name) LIKE '%serverless%compute%' OR LOWER(sku_name) LIKE '%interactive%'
# MAGIC       THEN 'Serverless Compute'
# MAGIC     ELSE 'Outros'
# MAGIC   END AS categoria_workload,
# MAGIC   SUM(usage_quantity) AS total_dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 100.0 / (SELECT SUM(usage_quantity) FROM system.billing.usage WHERE usage_date >= '2026-06-01'), 1) AS pct_total,
# MAGIC   COUNT(DISTINCT usage_date) AS dias_com_uso
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= '2026-06-01'
# MAGIC GROUP BY categoria_workload
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Custo Estimado por Categoria

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Custo estimado usando list_prices
# MAGIC SELECT
# MAGIC   u.sku_name,
# MAGIC   CASE
# MAGIC     WHEN LOWER(u.sku_name) LIKE '%dlt%' OR LOWER(u.sku_name) LIKE '%pipelines%'
# MAGIC       THEN 'DLT Pipelines'
# MAGIC     WHEN LOWER(u.sku_name) LIKE '%serving%' OR LOWER(u.sku_name) LIKE '%model%'
# MAGIC       THEN 'Model Serving'
# MAGIC     WHEN LOWER(u.sku_name) LIKE '%sql%' OR LOWER(u.sku_name) LIKE '%warehouse%'
# MAGIC       THEN 'SQL Warehouse'
# MAGIC     WHEN LOWER(u.sku_name) LIKE '%serverless%compute%' OR LOWER(u.sku_name) LIKE '%interactive%'
# MAGIC       THEN 'Serverless Compute'
# MAGIC     ELSE 'Outros'
# MAGIC   END AS categoria,
# MAGIC   SUM(u.usage_quantity) AS total_dbus,
# MAGIC   ROUND(p.pricing.default, 4) AS preco_por_dbu,
# MAGIC   ROUND(SUM(u.usage_quantity) * p.pricing.default, 2) AS custo_estimado_usd
# MAGIC FROM system.billing.usage u
# MAGIC LEFT JOIN system.billing.list_prices p
# MAGIC   ON u.sku_name = p.sku_name
# MAGIC   AND u.usage_date BETWEEN p.price_start_time AND COALESCE(p.price_end_time, '2099-12-31')
# MAGIC WHERE u.usage_date >= '2026-06-01'
# MAGIC GROUP BY u.sku_name, categoria, p.pricing.default
# MAGIC ORDER BY custo_estimado_usd DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Eventos DLT — Pipeline Event Log

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Eventos dos pipelines DLT (Silver + CDC)
# MAGIC SELECT
# MAGIC   pipeline_name,
# MAGIC   event_type,
# MAGIC   level,
# MAGIC   message,
# MAGIC   timestamp
# MAGIC FROM system.lakeflow.pipeline_event_log
# MAGIC WHERE timestamp >= '2026-06-01'
# MAGIC   AND pipeline_name LIKE '%brasilmart%'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 30

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Resumo de execuções DLT por pipeline
# MAGIC SELECT
# MAGIC   pipeline_name,
# MAGIC   event_type,
# MAGIC   level,
# MAGIC   COUNT(*) AS total_eventos,
# MAGIC   MIN(timestamp) AS primeiro_evento,
# MAGIC   MAX(timestamp) AS ultimo_evento
# MAGIC FROM system.lakeflow.pipeline_event_log
# MAGIC WHERE timestamp >= '2026-06-01'
# MAGIC   AND pipeline_name LIKE '%brasilmart%'
# MAGIC GROUP BY pipeline_name, event_type, level
# MAGIC ORDER BY pipeline_name, total_eventos DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Inventário de Compute

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clusters e compute utilizados pelo workspace
# MAGIC SELECT
# MAGIC   cluster_id,
# MAGIC   cluster_name,
# MAGIC   cluster_source,
# MAGIC   driver_node_type,
# MAGIC   worker_node_type,
# MAGIC   autoscale_min_workers,
# MAGIC   autoscale_max_workers,
# MAGIC   state,
# MAGIC   create_time
# MAGIC FROM system.compute.clusters
# MAGIC WHERE delete_time IS NULL
# MAGIC ORDER BY create_time DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Resumo Analítico com Python

# COMMAND ----------

from pyspark.sql import functions as F

df_usage = spark.sql("""
    SELECT
        usage_date,
        sku_name,
        usage_quantity,
        usage_metadata.cluster_id,
        usage_metadata.warehouse_id,
        usage_metadata.job_id,
        CASE
            WHEN LOWER(sku_name) LIKE '%dlt%' OR LOWER(sku_name) LIKE '%pipelines%'
                THEN 'DLT Pipelines'
            WHEN LOWER(sku_name) LIKE '%serving%' OR LOWER(sku_name) LIKE '%model%'
                THEN 'Model Serving'
            WHEN LOWER(sku_name) LIKE '%sql%' OR LOWER(sku_name) LIKE '%warehouse%'
                THEN 'SQL Warehouse'
            WHEN LOWER(sku_name) LIKE '%serverless%compute%' OR LOWER(sku_name) LIKE '%interactive%'
                THEN 'Serverless Compute'
            ELSE 'Outros'
        END AS categoria
    FROM system.billing.usage
    WHERE usage_date >= '2026-06-01'
""")

total_dbus = df_usage.agg(F.sum("usage_quantity")).first()[0] or 0
total_dias = df_usage.select("usage_date").distinct().count()

print("=" * 65)
print("RESUMO — Consumo de DBUs (System Tables)")
print("=" * 65)
print(f"\nPeríodo: 2026-06-01 a hoje")
print(f"Total de DBUs consumidos: {total_dbus:,.2f}")
print(f"Dias com uso: {total_dias}")
if total_dias > 0:
    print(f"Média diária: {total_dbus / total_dias:,.2f} DBUs/dia")

print(f"\n{'Categoria':<25} {'DBUs':>12} {'% Total':>10}")
print("-" * 50)

resumo = (
    df_usage.groupBy("categoria")
    .agg(F.sum("usage_quantity").alias("total"))
    .orderBy(F.desc("total"))
    .collect()
)

for row in resumo:
    pct = row["total"] / total_dbus * 100 if total_dbus > 0 else 0
    print(f"  {row['categoria']:<23} {row['total']:>12,.2f} {pct:>9.1f}%")

# COMMAND ----------

# SKUs detalhados
print(f"\n{'SKU':<55} {'DBUs':>12}")
print("-" * 70)

skus = (
    df_usage.groupBy("sku_name")
    .agg(F.sum("usage_quantity").alias("total"))
    .orderBy(F.desc("total"))
    .collect()
)

for row in skus:
    print(f"  {row['sku_name']:<53} {row['total']:>12,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Evidências

# COMMAND ----------

print("=" * 70)
print("EVIDÊNCIAS — TP5 Atividade 3.2: System Tables — DBUs")
print("=" * 70)
print(f"""
1. SYSTEM TABLES CONSULTADAS:
   - system.billing.usage (consumo de DBUs por SKU e dia)
   - system.billing.list_prices (preço por DBU para custo estimado)
   - system.lakeflow.pipeline_event_log (eventos DLT)
   - system.compute.clusters (inventário de compute)

2. CATEGORIAS DE WORKLOAD ANALISADAS:
   - DLT Pipelines (pb-brasilmart-silver, pb-brasilmart-silver-cdc)
   - Model Serving (pb-brasilmart-atraso-endpoint)
   - Serverless Compute (notebooks TP1→TP5)
   - SQL Warehouse (DBSQL queries, GenAI functions)

3. MÉTRICAS CAPTURADAS:
   - Total DBUs por categoria e SKU
   - Série temporal diária (consumo por dia)
   - Custo estimado em USD (DBUs × list_price)
   - Eventos DLT (execuções, status, duração)
   - Percentual de cada workload no consumo total

4. COMO VERIFICAR NO DATABRICKS:
   SQL Editor → Executar as queries deste notebook
   system.billing.usage é acessível para admins do workspace
   Console → Billing → Usage → filtrar por workspace
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo TP5 — Atividade 3.2
# MAGIC
# MAGIC ### System Tables Utilizadas
# MAGIC | System Table | Dados Extraídos |
# MAGIC |-------------|----------------|
# MAGIC | `system.billing.usage` | DBUs por SKU, cluster, warehouse, dia |
# MAGIC | `system.billing.list_prices` | Preço por DBU para estimativa de custo |
# MAGIC | `system.lakeflow.pipeline_event_log` | Eventos dos pipelines DLT |
# MAGIC | `system.compute.clusters` | Inventário de clusters/compute |
# MAGIC
# MAGIC ### Análises Realizadas
# MAGIC | Análise | Detalhe |
# MAGIC |---------|--------|
# MAGIC | Consumo total por SKU | Todas as categorias de DBU do workspace |
# MAGIC | DLT Pipelines | DBUs dos pipelines Silver e CDC |
# MAGIC | Model Serving | DBUs do endpoint de predição de atraso |
# MAGIC | Serverless Compute | DBUs dos notebooks interativos (ML training, Feedback Loop) |
# MAGIC | SQL Warehouse | DBUs do DBSQL (GenAI, queries ad-hoc) |
# MAGIC | Série temporal | Consumo diário por categoria |
# MAGIC | Custo estimado | DBUs × list_price (USD) |
# MAGIC
# MAGIC ### Fluxo
# MAGIC ```
# MAGIC Workloads (DLT, ML, SQL, Serving)
# MAGIC   → System Tables (billing.usage)
# MAGIC     → Análise por SKU/categoria/dia
# MAGIC       → Custo estimado (billing.list_prices)
# MAGIC         → Otimização de recursos
# MAGIC ```
