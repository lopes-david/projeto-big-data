# Databricks notebook source
# MAGIC %md
# MAGIC # TP2 — 3.1 Monitoramento de Recursos AWS e Databricks
# MAGIC
# MAGIC **Objetivo:** Apresentar evidências de uso e monitoramento dos recursos
# MAGIC utilizados no projeto BrasilMart.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Uso de Storage — S3 por Camada
# MAGIC
# MAGIC | Camada | Bucket | Objetos | Tamanho |
# MAGIC |--------|--------|---------|---------|
# MAGIC | **Raw** | pb-raw-brasilmart-234828142988 | 10 | 137 MB |
# MAGIC | **Bronze** | pb-bronze-brasilmart-234828142988 | 2.462 | 19.6 MB |
# MAGIC | **Silver** | pb-silver-brasilmart-234828142988 | 0 | 0 MB |
# MAGIC | **Gold** | pb-gold-brasilmart-234828142988 | 0 | 0 MB |
# MAGIC
# MAGIC **Nota:** Silver e Gold serão populados nas próximas etapas (dbt → Redshift).
# MAGIC Bronze tem mais objetos que Raw porque o Delta Lake fragmenta em múltiplos Parquet + `_delta_log`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. AWS Glue — Histórico de Jobs
# MAGIC
# MAGIC | Job | Status | Duração | Workers | Versão |
# MAGIC |-----|--------|---------|---------|--------|
# MAGIC | pb-batch-ingestion-orders | SUCCEEDED | 3m25s | 2 × G.1X | Glue 4.0 |
# MAGIC | pb-batch-ingestion-orders | FAILED (1ª) | 40s | 2 × G.1X | Glue 4.0 |
# MAGIC | pb-batch-ingestion-orders | FAILED (2ª) | 48s | 2 × G.1X | Glue 4.0 |
# MAGIC
# MAGIC **Causa dos falhos:** GlueETLRole não tinha permissão IAM nos novos buckets `pb-*`.
# MAGIC Corrigido com inline policy `PBBrasilmartS3Access`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Redshift Serverless
# MAGIC
# MAGIC | Parâmetro | Valor |
# MAGIC |-----------|-------|
# MAGIC | Workgroup | default-workgroup |
# MAGIC | Status | AVAILABLE |
# MAGIC | Base Capacity | 128 RPUs |
# MAGIC | Modelo | Pay-per-query (serverless) |
# MAGIC
# MAGIC O Redshift Serverless escala automaticamente e cobra apenas por consulta executada.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Databricks — Compute

# COMMAND ----------

# SQL Warehouse
print("=== Databricks SQL Warehouse ===")
print(f"  Nome:       Serverless Starter Warehouse")
print(f"  Tipo:       PRO (Serverless)")
print(f"  Tamanho:    2X-Small")
print(f"  Auto-stop:  10 minutos")
print(f"  Photon:     Habilitado")
print(f"  Custo:      Pay-per-use (sem cluster ocioso)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Databricks — Tabelas no Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG pb_brasilmart;

# COMMAND ----------

tables = ["orders", "customers", "items", "payments", "reviews", "products", "sellers", "geolocation", "category_translation"]

print(f"{'Tabela':<25} {'Registros':>12} {'Formato':<8} {'Tipo':<10}")
print("-" * 60)

for t in tables:
    try:
        count = spark.table(f"bronze.{t}").count()
        detail = spark.sql(f"DESCRIBE DETAIL bronze.{t}").first()
        fmt = detail.format if detail else "delta"
        ttype = detail.tableType if hasattr(detail, 'tableType') else "MANAGED"
        print(f"{t:<25} {count:>12,} {fmt:<8} {ttype:<10}")
    except Exception as e:
        print(f"{t:<25} {'ERRO':<12}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Delta Lake — Tamanho das Tabelas

# COMMAND ----------

print(f"{'Tabela':<25} {'Arquivos':>10} {'Tamanho (MB)':>15}")
print("-" * 55)

total_files = 0
total_size = 0

for t in tables:
    try:
        d = spark.sql(f"DESCRIBE DETAIL bronze.{t}").first()
        size_mb = d.sizeInBytes / 1024 / 1024
        total_files += d.numFiles
        total_size += size_mb
        print(f"{t:<25} {d.numFiles:>10} {size_mb:>15.2f}")
    except:
        print(f"{t:<25} {'N/A':>10}")

print("-" * 55)
print(f"{'TOTAL':<25} {total_files:>10} {total_size:>15.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Histórico de Operações Delta (Auditoria)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY pb_brasilmart.bronze.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Lake Formation — Governança

# COMMAND ----------

# MAGIC %md
# MAGIC | Recurso | Configuração |
# MAGIC |---------|-------------|
# MAGIC | **Data Lake Admin** | root (Full Access) |
# MAGIC | **Buckets registrados** | 4 (pb-raw, pb-bronze, pb-silver, pb-gold) |
# MAGIC | **Databases Glue** | 4 (pb_raw, pb_bronze, pb_silver, pb_gold) |
# MAGIC | **Permissões GlueETLRole** | DATA_LOCATION_ACCESS em raw + bronze |
# MAGIC | **Criptografia** | SSE-S3 em todos os buckets |
# MAGIC | **Versionamento** | Habilitado em todos os buckets |
# MAGIC | **Lifecycle** | Raw=7 anos, Bronze=3 anos, Silver=2 anos, Gold=1 ano |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo de Custos Estimados
# MAGIC
# MAGIC | Serviço | Uso | Custo Estimado |
# MAGIC |---------|-----|----------------|
# MAGIC | **S3** | ~157 MB total | ~$0.01/mês |
# MAGIC | **Glue** | 1 job (3m25s, 2 DPU) | ~$0.15 |
# MAGIC | **Redshift Serverless** | 128 RPU base | Pay-per-query |
# MAGIC | **Databricks** | Serverless Starter | Pay-per-use |
# MAGIC | **Lake Formation** | Governança | Sem custo adicional |
# MAGIC
# MAGIC **Estratégias de economia aplicadas:**
# MAGIC - Serverless em tudo (Redshift, Databricks) — sem cluster ocioso
# MAGIC - Auto-stop de 10 min no SQL Warehouse
# MAGIC - Lifecycle policies no S3 (Standard-IA → Glacier)
# MAGIC - Glue com 2 workers G.1X (mínimo suficiente)
