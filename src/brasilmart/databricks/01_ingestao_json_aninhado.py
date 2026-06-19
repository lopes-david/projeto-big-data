# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 01 — Ingestão de JSON Aninhado (Pedido Unificado BrasilMart)
# MAGIC
# MAGIC **Objetivo:** Ingerir documentos JSON com 4 níveis de aninhamento representando
# MAGIC pedidos unificados da Olist (order + customer + items[] + payments[] + review{}),
# MAGIC explodir as estruturas e persistir em Delta Lake na camada Bronze.
# MAGIC
# MAGIC **Fonte:** `s3://brasilmart-raw-dev/orders_json/orders_unified.jsonl`
# MAGIC **Destino:** `s3://brasilmart-bronze-dev/orders_json/`
# MAGIC
# MAGIC **Dados:** 99.441 pedidos da Olist (2016–2018), com até N itens e N pagamentos por pedido.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

RAW_PATH = "s3://brasilmart-raw-dev/orders_json/orders_unified.jsonl"
BRONZE_PATH = "s3://brasilmart-bronze-dev/orders_json/"

# Para desenvolvimento local com os arquivos copiados:
# RAW_PATH = "/home/davidl/projeto-big-data/data/raw/orders_json/orders_unified.jsonl"
# BRONZE_PATH = "/home/davidl/projeto-big-data/data/bronze/orders_json/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema do JSON Aninhado
# MAGIC
# MAGIC Estrutura: `order (raiz) → customer{} + timestamps{} + items[] + payments[] + review{}`

# COMMAND ----------

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("zip_code_prefix", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
])

timestamps_schema = StructType([
    StructField("purchase", StringType(), True),
    StructField("approved", StringType(), True),
    StructField("delivered_carrier", StringType(), True),
    StructField("delivered_customer", StringType(), True),
    StructField("estimated_delivery", StringType(), True),
])

item_schema = StructType([
    StructField("order_item_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True),
])

payment_schema = StructType([
    StructField("sequential", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("installments", IntegerType(), True),
    StructField("value", DoubleType(), True),
])

review_schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("comment_title", StringType(), True),
    StructField("comment_message", StringType(), True),
    StructField("creation_date", StringType(), True),
    StructField("answer_timestamp", StringType(), True),
])

order_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_status", StringType(), True),
    StructField("customer", customer_schema, True),
    StructField("timestamps", timestamps_schema, True),
    StructField("items", ArrayType(item_schema), True),
    StructField("payments", ArrayType(payment_schema), True),
    StructField("review", review_schema, True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Leitura do JSON Aninhado

# COMMAND ----------

df_raw = spark.read.schema(order_schema).json(RAW_PATH)

print(f"Pedidos lidos: {df_raw.count():,}")
print("\nSchema:")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Explodir Items[] — 1 pedido → N linhas

# COMMAND ----------

df_with_items = (
    df_raw.select(
        "order_id",
        "order_status",
        F.col("customer.customer_id").alias("customer_id"),
        F.col("customer.customer_unique_id").alias("customer_unique_id"),
        F.col("customer.zip_code_prefix").alias("customer_zip"),
        F.col("customer.city").alias("customer_city"),
        F.col("customer.state").alias("customer_state"),
        F.to_timestamp("timestamps.purchase").alias("purchase_ts"),
        F.to_timestamp("timestamps.approved").alias("approved_ts"),
        F.to_timestamp("timestamps.delivered_carrier").alias("delivered_carrier_ts"),
        F.to_timestamp("timestamps.delivered_customer").alias("delivered_customer_ts"),
        F.to_timestamp("timestamps.estimated_delivery").alias("estimated_delivery_ts"),
        F.explode_outer("items").alias("item"),
        "review",
    )
    .select(
        "*",
        F.col("item.order_item_id").alias("order_item_id"),
        F.col("item.product_id").alias("product_id"),
        F.col("item.seller_id").alias("seller_id"),
        F.col("item.price").alias("price"),
        F.col("item.freight_value").alias("freight_value"),
    )
    .drop("item")
)

print(f"Após explode de items: {df_with_items.count():,} linhas (pedidos × itens)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Tabela de Pagamentos (separada — N pagamentos por pedido)

# COMMAND ----------

df_payments = (
    df_raw.select("order_id", F.explode_outer("payments").alias("pmt"))
    .select(
        "order_id",
        F.col("pmt.sequential").alias("payment_sequential"),
        F.col("pmt.type").alias("payment_type"),
        F.col("pmt.installments").alias("payment_installments"),
        F.col("pmt.value").alias("payment_value"),
    )
)

print(f"Pagamentos: {df_payments.count():,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Enriquecer com Total do Pedido (soma de pagamentos)

# COMMAND ----------

df_order_total = (
    df_payments.groupBy("order_id")
    .agg(
        F.sum("payment_value").alias("total_order_value"),
        F.collect_list("payment_type").alias("payment_types"),
        F.max("payment_installments").alias("max_installments"),
        F.count("*").alias("num_payment_methods"),
    )
)

df_bronze_orders = df_with_items.join(df_order_total, on="order_id", how="left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Extrair Dados do Review

# COMMAND ----------

df_bronze_orders = (
    df_bronze_orders.withColumn(
        "review_score", F.col("review.score")
    )
    .withColumn("review_comment", F.col("review.comment_message"))
    .withColumn(
        "review_creation_date",
        F.to_timestamp(F.col("review.creation_date")),
    )
    .drop("review")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Derivar Métricas de Logística

# COMMAND ----------

df_bronze_orders = (
    df_bronze_orders
    .withColumn(
        "delta_entrega_dias",
        F.datediff("delivered_customer_ts", "estimated_delivery_ts"),
    )
    .withColumn(
        "entregue_no_prazo",
        F.when(F.col("delta_entrega_dias") <= 0, True).otherwise(False),
    )
    .withColumn(
        "purchase_date",
        F.to_date("purchase_ts"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Adicionar Metadados e Persistir na Bronze (Delta Lake)

# COMMAND ----------

df_bronze_final = (
    df_bronze_orders
    .withColumn("_ingestao_timestamp", F.current_timestamp())
    .withColumn("_notebook", F.lit("01_ingestao_json_aninhado"))
)

(
    df_bronze_final.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("purchase_date")
    .save(BRONZE_PATH)
)

print(f"Dados salvos na Bronze: {BRONZE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Verificação

# COMMAND ----------

df_check = spark.read.format("delta").load(BRONZE_PATH)

print(f"Registros na Bronze: {df_check.count():,}")
print(f"Período: {df_check.agg(F.min('purchase_date'), F.max('purchase_date')).collect()[0]}")
print(f"Status de pedidos:")
display(df_check.groupBy("order_status").count().orderBy(F.desc("count")))

# COMMAND ----------

# Verificar distribuição de pagamentos
print("Top formas de pagamento:")
display(
    df_payments.groupBy("payment_type")
    .agg(F.count("*").alias("total"), F.avg("payment_value").alias("avg_value"))
    .orderBy(F.desc("total"))
)
