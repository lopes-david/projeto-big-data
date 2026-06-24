# Databricks notebook source
# MAGIC %md
# MAGIC # TP3 — 3.1 Transferência Silver → Amazon Redshift
# MAGIC
# MAGIC **Objetivo:** Exportar as tabelas Silver finalizadas no Databricks (Unity Catalog)
# MAGIC para o esquema `raw_databricks` no Amazon Redshift Serverless, usando o conector
# MAGIC Spark-Redshift com staging via S3.
# MAGIC
# MAGIC **Fluxo:**
# MAGIC ```
# MAGIC Databricks Silver (pb_brasilmart.silver.*)
# MAGIC   → S3 staging (Parquet temporário)
# MAGIC     → Redshift COPY (schema raw_databricks)
# MAGIC ```
# MAGIC
# MAGIC **Pré-requisitos:**
# MAGIC - IAM Role `redshift-s3-copy-role` com acesso ao bucket de staging
# MAGIC - Conector `io.databricks.spark.redshift` disponível no cluster
# MAGIC - Schema `raw_databricks` criado no Redshift

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuração

# COMMAND ----------

REDSHIFT_HOST = "default-workgroup.234828142988.sa-east-1.redshift-serverless.amazonaws.com"
REDSHIFT_PORT = "5439"
REDSHIFT_DB = "dev"
REDSHIFT_SCHEMA = "raw_databricks"
REDSHIFT_USER = dbutils.secrets.get(scope="pb-brasilmart", key="redshift-user")
REDSHIFT_PASS = dbutils.secrets.get(scope="pb-brasilmart", key="redshift-password")

REDSHIFT_URL = f"jdbc:redshift://{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}"

S3_STAGING = "s3://pb-silver-brasilmart-234828142988/redshift-staging"
IAM_ROLE = "arn:aws:iam::234828142988:role/redshift-s3-copy-role"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Criar Schema no Redshift

# COMMAND ----------

from pyspark.sql import functions as F

create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {REDSHIFT_SCHEMA}"

spark.read \
    .format("io.databricks.spark.redshift") \
    .option("url", REDSHIFT_URL) \
    .option("user", REDSHIFT_USER) \
    .option("password", REDSHIFT_PASS) \
    .option("query", f"SELECT 1") \
    .option("tempdir", S3_STAGING) \
    .option("aws_iam_role", IAM_ROLE) \
    .load()

print(f"Conexão com Redshift verificada: {REDSHIFT_HOST}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Exportar Tabelas Silver para Redshift
# MAGIC
# MAGIC Transfere as 7 tabelas Silver principais (sem geolocation e category_translation)
# MAGIC e as 2 tabelas enriquecidas.

# COMMAND ----------

SILVER_TABLES = {
    "orders": {
        "distkey": "order_id",
        "sortkey": "order_purchase_timestamp"
    },
    "customers": {
        "distkey": "customer_id",
        "sortkey": "customer_state"
    },
    "items": {
        "distkey": "order_id",
        "sortkey": "product_id"
    },
    "payments": {
        "distkey": "order_id",
        "sortkey": "payment_type"
    },
    "reviews": {
        "distkey": "order_id",
        "sortkey": "review_score"
    },
    "products": {
        "distkey": "product_id",
        "sortkey": "product_category"
    },
    "sellers": {
        "distkey": "seller_id",
        "sortkey": "seller_state"
    },
    "orders_enriched": {
        "distkey": "order_id",
        "sortkey": "order_purchase_timestamp"
    },
    "items_enriched": {
        "distkey": "order_id",
        "sortkey": "product_category"
    },
}

# COMMAND ----------

resultados = []

for table_name, keys in SILVER_TABLES.items():
    print(f"\nExportando: silver.{table_name} → {REDSHIFT_SCHEMA}.{table_name}")

    df = spark.table(f"pb_brasilmart.silver.{table_name}")
    count = df.count()

    (df.write
     .format("io.databricks.spark.redshift")
     .option("url", REDSHIFT_URL)
     .option("user", REDSHIFT_USER)
     .option("password", REDSHIFT_PASS)
     .option("dbtable", f"{REDSHIFT_SCHEMA}.{table_name}")
     .option("tempdir", S3_STAGING)
     .option("aws_iam_role", IAM_ROLE)
     .option("distkey", keys["distkey"])
     .option("sortkeyspec", f"SORTKEY({keys['sortkey']})")
     .mode("overwrite")
     .save()
    )

    resultados.append({"tabela": table_name, "registros": count, "status": "OK"})
    print(f"  ✓ {count:,} registros transferidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificação — Contagens no Redshift

# COMMAND ----------

print(f"\n{'Tabela':<25} {'Databricks':>12} {'Redshift':>12} {'Match':>8}")
print("-" * 62)

for item in resultados:
    table_name = item["tabela"]

    df_rs = (spark.read
        .format("io.databricks.spark.redshift")
        .option("url", REDSHIFT_URL)
        .option("user", REDSHIFT_USER)
        .option("password", REDSHIFT_PASS)
        .option("query", f"SELECT COUNT(*) AS cnt FROM {REDSHIFT_SCHEMA}.{table_name}")
        .option("tempdir", S3_STAGING)
        .option("aws_iam_role", IAM_ROLE)
        .load()
    )

    rs_count = df_rs.collect()[0]["cnt"]
    db_count = item["registros"]
    match = "OK" if rs_count == db_count else "DIFF"

    print(f"{table_name:<25} {db_count:>12,} {rs_count:>12,} {match:>8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo
# MAGIC
# MAGIC | Aspecto | Implementação |
# MAGIC |---------|--------------|
# MAGIC | **Conector** | `io.databricks.spark.redshift` (Spark-Redshift) |
# MAGIC | **Staging** | S3 `pb-silver-brasilmart-234828142988/redshift-staging` (Parquet temporário) |
# MAGIC | **Autenticação** | IAM Role `redshift-s3-copy-role` para COPY |
# MAGIC | **Destino** | Redshift schema `raw_databricks` (9 tabelas) |
# MAGIC | **Chaves** | DistKey + SortKey por tabela para performance |
# MAGIC | **Verificação** | Contagens cruzadas Databricks vs Redshift |
