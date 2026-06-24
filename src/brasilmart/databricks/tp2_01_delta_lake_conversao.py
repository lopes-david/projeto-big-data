# Databricks notebook source
# MAGIC %md
# MAGIC # TP2 — 1.1 Conversão para Delta Lake e Transacionalidade
# MAGIC
# MAGIC **Objetivo:** Converter os dados CSV da camada Raw para Delta Lake na Bronze,
# MAGIC evidenciando o `_delta_log` e como ele garante transacionalidade ACID.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

RAW_PATH = "s3://pb-raw-brasilmart-234828142988/olist"
BRONZE_PATH = "s3://pb-bronze-brasilmart-234828142988/delta"

DATASETS = {
    "orders":     f"{RAW_PATH}/olist_orders_dataset.csv",
    "customers":  f"{RAW_PATH}/olist_customers_dataset.csv",
    "items":      f"{RAW_PATH}/olist_order_items_dataset.csv",
    "payments":   f"{RAW_PATH}/olist_order_payments_dataset.csv",
    "reviews":    f"{RAW_PATH}/olist_order_reviews_dataset.csv",
    "products":   f"{RAW_PATH}/olist_products_dataset.csv",
    "sellers":    f"{RAW_PATH}/olist_sellers_dataset.csv",
    "geolocation": f"{RAW_PATH}/olist_geolocation_dataset.csv",
    "category_translation": f"{RAW_PATH}/product_category_name_translation.csv",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Conversão CSV → Delta Lake (Raw → Bronze)
# MAGIC
# MAGIC Cada CSV é lido, recebe metadados de ingestão e é salvo como Delta Lake.

# COMMAND ----------

for table_name, csv_path in DATASETS.items():
    print(f"Convertendo: {table_name}")

    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .option("quote", '"')
          .option("escape", '"')
          .csv(csv_path))

    df_bronze = (df
                 .withColumn("_ingestao_timestamp", F.current_timestamp())
                 .withColumn("_source_file", F.lit(csv_path)))

    delta_path = f"{BRONZE_PATH}/{table_name}"

    (df_bronze.write
     .format("delta")
     .mode("overwrite")
     .save(delta_path))

    count = spark.read.format("delta").load(delta_path).count()
    print(f"  ✓ {table_name}: {count} registros em {delta_path}")

print("\n✅ Todos os datasets convertidos para Delta Lake!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Evidência do `_delta_log`
# MAGIC
# MAGIC O Delta Lake armazena um **log de transações** no diretório `_delta_log/`.
# MAGIC Cada operação (write, update, delete, merge) gera um arquivo JSON numerado
# MAGIC sequencialmente (`00000000000000000000.json`, `00000000000000000001.json`, etc.).
# MAGIC
# MAGIC Esse log é o que garante as propriedades **ACID**:
# MAGIC - **Atomicidade:** cada commit é tudo-ou-nada
# MAGIC - **Consistência:** o log valida schema antes de aceitar novos dados
# MAGIC - **Isolamento:** leitores veem apenas commits completos
# MAGIC - **Durabilidade:** o log persiste no S3 junto com os dados

# COMMAND ----------

# Lista os arquivos do _delta_log da tabela orders
delta_log_path = f"{BRONZE_PATH}/orders/_delta_log/"
log_files = dbutils.fs.ls(delta_log_path)

print("Arquivos no _delta_log/orders:")
print("-" * 60)
for f in log_files:
    print(f"  {f.name}  ({f.size} bytes)")

# COMMAND ----------

# Lê o conteúdo do primeiro commit (JSON)
first_commit = spark.read.json(f"{delta_log_path}00000000000000000000.json")
display(first_commit)

# COMMAND ----------

# MAGIC %md
# MAGIC ### O que cada campo do commit significa:
# MAGIC - **commitInfo**: timestamp, operação (WRITE), quem executou
# MAGIC - **add**: arquivos Parquet adicionados neste commit (path, size, partitionValues)
# MAGIC - **metaData**: schema da tabela, formato, configurações
# MAGIC - **protocol**: versão mínima de leitor/escritor para compatibilidade

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Demonstração de Transacionalidade — Time Travel

# COMMAND ----------

# Versão 0: dados originais
orders_v0 = spark.read.format("delta").option("versionAsOf", 0).load(f"{BRONZE_PATH}/orders")
print(f"Versão 0 (original): {orders_v0.count()} registros")

# COMMAND ----------

# Faz um APPEND com dados duplicados (simula uma nova ingestão)
sample = spark.read.format("delta").load(f"{BRONZE_PATH}/orders").limit(100)

(sample.write
 .format("delta")
 .mode("append")
 .save(f"{BRONZE_PATH}/orders"))

# COMMAND ----------

# Agora temos versão 1 com mais registros
orders_v1 = spark.read.format("delta").load(f"{BRONZE_PATH}/orders")
print(f"Versão 1 (após append): {orders_v1.count()} registros")

# Time Travel: voltando para versão 0
orders_v0_again = spark.read.format("delta").option("versionAsOf", 0).load(f"{BRONZE_PATH}/orders")
print(f"Versão 0 (time travel): {orders_v0_again.count()} registros")

# COMMAND ----------

# Mostra o histórico de operações
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, f"{BRONZE_PATH}/orders")
display(dt.history())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Evidência — Novos arquivos no `_delta_log` após o append

# COMMAND ----------

log_files_after = dbutils.fs.ls(delta_log_path)
print("Arquivos no _delta_log/orders APÓS append:")
print("-" * 60)
for f in log_files_after:
    print(f"  {f.name}  ({f.size} bytes)")

print(f"\nAntes: 1 commit | Depois: {len(log_files_after)} commits")
print("Cada arquivo .json é um commit ACID completo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusão
# MAGIC
# MAGIC O `_delta_log` é o **coração do Delta Lake**. Ele:
# MAGIC 1. Registra cada operação como um commit atômico (arquivo JSON numerado)
# MAGIC 2. Permite **Time Travel** — acessar qualquer versão anterior dos dados
# MAGIC 3. Garante que leituras nunca veem dados parciais (isolamento)
# MAGIC 4. Valida schema automaticamente (consistência)
# MAGIC
# MAGIC Isso transforma o S3 (storage simples) em um **storage transacional** equivalente
# MAGIC a um banco de dados, sem precisar de um servidor de banco.

# COMMAND ----------

# Restaura versão 0 (remove os dados duplicados do append de teste)
dt.restoreToVersion(0)
print(f"✅ Tabela orders restaurada para versão 0: {spark.read.format('delta').load(f'{BRONZE_PATH}/orders').count()} registros")
