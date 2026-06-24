# Databricks notebook source
# MAGIC %md
# MAGIC # TP2 — 1.3 Camada Bronze Gerenciada pelo Unity Catalog
# MAGIC
# MAGIC **Objetivo:** Implementar a camada Bronze como **Managed Tables** no Unity Catalog,
# MAGIC com metadados de ingestão, qualidade básica e rastreabilidade.
# MAGIC
# MAGIC A diferença entre o notebook anterior (1.1) e este:
# MAGIC - 1.1 converteu CSVs para Delta com path externo (External Tables)
# MAGIC - Aqui criamos **Managed Tables** — o Unity Catalog controla localização, permissões e lifecycle

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG pb_brasilmart;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingestão com Metadados de Rastreabilidade
# MAGIC
# MAGIC Cada registro recebe:
# MAGIC - `_ingestao_ts`: quando foi ingerido
# MAGIC - `_fonte`: arquivo de origem
# MAGIC - `_versao_ingestao`: identificador do batch

# COMMAND ----------

RAW_PATH = "s3://pb-raw-brasilmart-234828142988/olist"
VERSAO = "2026-06-18_v1"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Orders (Pedidos)

# COMMAND ----------

df_orders = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW_PATH}/olist_orders_dataset.csv")
    .withColumn("_ingestao_ts", F.current_timestamp())
    .withColumn("_fonte", F.lit("olist_orders_dataset.csv"))
    .withColumn("_versao_ingestao", F.lit(VERSAO))
)

(df_orders.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("pb_brasilmart.bronze.orders"))

print(f"✓ orders: {spark.table('bronze.orders').count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Customers (Clientes)

# COMMAND ----------

df_customers = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW_PATH}/olist_customers_dataset.csv")
    .withColumn("_ingestao_ts", F.current_timestamp())
    .withColumn("_fonte", F.lit("olist_customers_dataset.csv"))
    .withColumn("_versao_ingestao", F.lit(VERSAO))
)

(df_customers.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("pb_brasilmart.bronze.customers"))

print(f"✓ customers: {spark.table('bronze.customers').count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Items (Itens de Pedido)

# COMMAND ----------

df_items = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW_PATH}/olist_order_items_dataset.csv")
    .withColumn("_ingestao_ts", F.current_timestamp())
    .withColumn("_fonte", F.lit("olist_order_items_dataset.csv"))
    .withColumn("_versao_ingestao", F.lit(VERSAO))
)

(df_items.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("pb_brasilmart.bronze.items"))

print(f"✓ items: {spark.table('bronze.items').count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Payments (Pagamentos)

# COMMAND ----------

df_payments = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW_PATH}/olist_order_payments_dataset.csv")
    .withColumn("_ingestao_ts", F.current_timestamp())
    .withColumn("_fonte", F.lit("olist_order_payments_dataset.csv"))
    .withColumn("_versao_ingestao", F.lit(VERSAO))
)

(df_payments.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("pb_brasilmart.bronze.payments"))

print(f"✓ payments: {spark.table('bronze.payments').count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Reviews (Avaliações)

# COMMAND ----------

df_reviews = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", '"')
    .option("escape", '"')
    .csv(f"{RAW_PATH}/olist_order_reviews_dataset.csv")
    .withColumn("_ingestao_ts", F.current_timestamp())
    .withColumn("_fonte", F.lit("olist_order_reviews_dataset.csv"))
    .withColumn("_versao_ingestao", F.lit(VERSAO))
)

(df_reviews.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("pb_brasilmart.bronze.reviews"))

print(f"✓ reviews: {spark.table('bronze.reviews').count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 Products (Produtos)

# COMMAND ----------

df_products = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW_PATH}/olist_products_dataset.csv")
    .withColumn("_ingestao_ts", F.current_timestamp())
    .withColumn("_fonte", F.lit("olist_products_dataset.csv"))
    .withColumn("_versao_ingestao", F.lit(VERSAO))
)

(df_products.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("pb_brasilmart.bronze.products"))

print(f"✓ products: {spark.table('bronze.products').count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.7 Sellers (Vendedores)

# COMMAND ----------

df_sellers = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW_PATH}/olist_sellers_dataset.csv")
    .withColumn("_ingestao_ts", F.current_timestamp())
    .withColumn("_fonte", F.lit("olist_sellers_dataset.csv"))
    .withColumn("_versao_ingestao", F.lit(VERSAO))
)

(df_sellers.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("pb_brasilmart.bronze.sellers"))

print(f"✓ sellers: {spark.table('bronze.sellers').count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.8 Geolocation

# COMMAND ----------

df_geo = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW_PATH}/olist_geolocation_dataset.csv")
    .withColumn("_ingestao_ts", F.current_timestamp())
    .withColumn("_fonte", F.lit("olist_geolocation_dataset.csv"))
    .withColumn("_versao_ingestao", F.lit(VERSAO))
)

(df_geo.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("pb_brasilmart.bronze.geolocation"))

print(f"✓ geolocation: {spark.table('bronze.geolocation').count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.9 Category Translation

# COMMAND ----------

df_cat = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW_PATH}/product_category_name_translation.csv")
    .withColumn("_ingestao_ts", F.current_timestamp())
    .withColumn("_fonte", F.lit("product_category_name_translation.csv"))
    .withColumn("_versao_ingestao", F.lit(VERSAO))
)

(df_cat.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("pb_brasilmart.bronze.category_translation"))

print(f"✓ category_translation: {spark.table('bronze.category_translation').count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificação — Tabelas Managed no Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN pb_brasilmart.bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Qualidade Básica — Contagens e Nulos por Tabela

# COMMAND ----------

tables = ["orders", "customers", "items", "payments", "reviews", "products", "sellers", "geolocation", "category_translation"]

print(f"{'Tabela':<25} {'Registros':>10} {'Colunas':>8} {'Nulos (total)':>15}")
print("-" * 62)

for t in tables:
    df = spark.table(f"bronze.{t}")
    total = df.count()
    cols = len(df.columns)
    null_count = 0
    for c in df.columns:
        if not c.startswith("_"):
            null_count += df.where(F.col(c).isNull()).count()
    print(f"{t:<25} {total:>10,} {cols:>8} {null_count:>15,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Detalhes de uma Managed Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED pb_brasilmart.bronze.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Prova que é Managed: Type = MANAGED
# MAGIC DESCRIBE DETAIL pb_brasilmart.bronze.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Histórico de Operações (Delta Log via Unity Catalog)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY pb_brasilmart.bronze.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo
# MAGIC
# MAGIC | Aspecto | Implementação |
# MAGIC |---------|--------------|
# MAGIC | **Catálogo** | `pb_brasilmart` |
# MAGIC | **Schema** | `bronze` (9 tabelas) |
# MAGIC | **Tipo** | Managed Tables — Unity Catalog controla dados e metadados |
# MAGIC | **Formato** | Delta Lake (ACID, Time Travel, Schema Enforcement) |
# MAGIC | **Rastreabilidade** | `_ingestao_ts`, `_fonte`, `_versao_ingestao` em cada registro |
# MAGIC | **Governança** | Permissões herdadas do catálogo via Unity Catalog |
