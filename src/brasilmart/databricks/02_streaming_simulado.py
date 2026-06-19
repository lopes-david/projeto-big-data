# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02 — Ingestão de Streaming Simulado (Pedidos BrasilMart)
# MAGIC
# MAGIC **Objetivo:** Simular chegada incremental de pedidos usando o dataset Olist
# MAGIC particionado por dia (2016–2018). Cada "janela de tempo" representa um dia
# MAGIC de novos pedidos chegando ao sistema — simula o padrão de streaming de
# MAGIC um marketplace recebendo novos pedidos em tempo real.
# MAGIC
# MAGIC **Fonte:** `s3://brasilmart-raw-dev/orders_json/streaming/YYYY-MM-DD/orders.jsonl`
# MAGIC **Destino:** `s3://brasilmart-bronze-dev/orders_streaming/`
# MAGIC **Checkpoint:** `s3://brasilmart-bronze-dev/_checkpoints/orders_streaming/`
# MAGIC
# MAGIC **Dados:** ~99.441 pedidos distribuídos em ~700 partições diárias (out/2016 a out/2018)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

STREAMING_RAW_PATH = "s3://brasilmart-raw-dev/orders_json/streaming/"
BRONZE_STREAMING_PATH = "s3://brasilmart-bronze-dev/orders_streaming/"
CHECKPOINT_PATH = "s3://brasilmart-bronze-dev/_checkpoints/orders_streaming/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema do JSON de Pedido (mesmo do Notebook 01)

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
    StructField("comment_message", StringType(), True),
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
# MAGIC ## 3. Leitura com Structured Streaming (Auto Loader)
# MAGIC
# MAGIC O Auto Loader detecta automaticamente novos arquivos JSON particionados por dia.
# MAGIC Cada novo diretório `YYYY-MM-DD/` é processado como um micro-batch.

# COMMAND ----------

df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema/")
    .option("cloudFiles.inferColumnTypes", "false")
    .schema(order_schema)
    .load(STREAMING_RAW_PATH)
)

print("Stream configurado. Schema:")
df_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Transformações em Streaming
# MAGIC
# MAGIC Extraímos campos essenciais do JSON e calculamos o valor total do pedido
# MAGIC (soma dos pagamentos) como métrica de GMV em tempo real.

# COMMAND ----------

df_enriched = (
    df_stream
    # Dados básicos do pedido
    .withColumn("purchase_ts", F.to_timestamp("timestamps.purchase"))
    .withColumn("estimated_delivery_ts", F.to_timestamp("timestamps.estimated_delivery"))
    .withColumn("customer_state", F.col("customer.state"))
    .withColumn("customer_city", F.col("customer.city"))
    .withColumn("customer_unique_id", F.col("customer.customer_unique_id"))
    # Total do pedido: somar array de pagamentos
    .withColumn(
        "total_order_value",
        F.aggregate(
            F.col("payments"),
            F.lit(0.0).cast(DoubleType()),
            lambda acc, x: acc + x["value"],
        ),
    )
    # Número de itens
    .withColumn("num_items", F.size("items"))
    # Flag pedido com review negativo
    .withColumn(
        "review_negativo",
        F.when(F.col("review.score") <= 2, True).otherwise(False),
    )
    # Partição de data para escrita
    .withColumn("purchase_date", F.to_date("purchase_ts"))
    # Metadados
    .withColumn("_ingestao_timestamp", F.current_timestamp())
    .withColumn("_notebook", F.lit("02_streaming_simulado"))
    # Selecionar colunas relevantes (sem structs aninhados)
    .select(
        "order_id",
        "order_status",
        "customer_unique_id",
        "customer_state",
        "customer_city",
        "purchase_ts",
        "estimated_delivery_ts",
        "total_order_value",
        "num_items",
        "review_negativo",
        F.col("review.score").alias("review_score"),
        "purchase_date",
        "_ingestao_timestamp",
        "_notebook",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Agregação por Janela de Tempo — GMV Diário por Estado
# MAGIC
# MAGIC Calcula métricas de negócio em janelas de 1 dia, agrupadas por estado.
# MAGIC Permite monitorar o GMV em "quasi-tempo-real" por região do país.

# COMMAND ----------

df_gmv_daily = (
    df_enriched
    .withWatermark("purchase_ts", "1 day")
    .groupBy(
        F.window("purchase_ts", "1 day").alias("window"),
        "customer_state",
    )
    .agg(
        F.count("order_id").alias("total_pedidos"),
        F.sum("total_order_value").alias("gmv"),
        F.avg("total_order_value").alias("ticket_medio"),
        F.sum(F.col("review_negativo").cast("int")).alias("reviews_negativos"),
        F.countDistinct("customer_unique_id").alias("clientes_unicos"),
    )
    .withColumn("data", F.col("window.start").cast("date"))
    .drop("window")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Escrita na Camada Bronze (Delta Streaming)

# COMMAND ----------

# Stream completo de pedidos individuais
query_pedidos = (
    df_enriched.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/pedidos/")
    .partitionBy("purchase_date")
    .trigger(availableNow=True)
    .start(BRONZE_STREAMING_PATH)
)

# Stream de GMV diário por estado (aggregation)
query_gmv = (
    df_gmv_daily.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/gmv_diario/")
    .trigger(availableNow=True)
    .start(f"{BRONZE_STREAMING_PATH}_gmv_diario/")
)

query_pedidos.awaitTermination()
query_gmv.awaitTermination()

print("Streaming batch concluído.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verificação dos Dados Ingeridos

# COMMAND ----------

df_check = spark.read.format("delta").load(BRONZE_STREAMING_PATH)
df_gmv = spark.read.format("delta").load(f"{BRONZE_STREAMING_PATH}_gmv_diario/")

print(f"Pedidos na Bronze (streaming): {df_check.count():,}")
print(f"Dias no GMV diário: {df_gmv.count():,}")

print("\nTop 5 estados por GMV total:")
display(
    df_gmv.groupBy("customer_state")
    .agg(F.sum("gmv").alias("gmv_total"), F.sum("total_pedidos").alias("pedidos"))
    .orderBy(F.desc("gmv_total"))
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Análise de Pico — Distribuição de Pedidos por Mês

# COMMAND ----------

print("Distribuição de pedidos por mês (sazonalidade):")
display(
    df_check.withColumn("mes", F.date_trunc("month", "purchase_ts"))
    .groupBy("mes")
    .agg(
        F.count("order_id").alias("total_pedidos"),
        F.sum("total_order_value").alias("gmv"),
        F.avg("total_order_value").alias("ticket_medio"),
    )
    .orderBy("mes")
)
