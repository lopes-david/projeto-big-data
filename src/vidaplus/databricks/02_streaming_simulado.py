# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02 — Ingestão de Streaming Simulado (Sinais Vitais IoT)
# MAGIC
# MAGIC **Objetivo:** Simular e ingerir dados de streaming de dispositivos IoT de
# MAGIC monitoramento de sinais vitais em UTI, utilizando Spark Structured Streaming
# MAGIC com Auto Loader.
# MAGIC
# MAGIC **Fonte de dados:** `s3://vidaplus-raw-dev/sinais_vitais/` (arquivos JSON chegando continuamente)
# MAGIC **Destino:** `s3://vidaplus-bronze-dev/sinais_vitais/`
# MAGIC **Checkpoint:** `s3://vidaplus-bronze-dev/_checkpoints/sinais_vitais/`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
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

RAW_PATH = "s3://vidaplus-raw-dev/sinais_vitais/"
BRONZE_PATH = "s3://vidaplus-bronze-dev/sinais_vitais/"
CHECKPOINT_PATH = "s3://vidaplus-bronze-dev/_checkpoints/sinais_vitais/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema dos Sinais Vitais IoT

# COMMAND ----------

sinais_vitais_schema = StructType(
    [
        StructField("device_id", StringType(), False),
        StructField("paciente_id", StringType(), False),
        StructField("leito", StringType(), True),
        StructField("unidade_id", StringType(), True),
        StructField("timestamp", TimestampType(), False),
        StructField("frequencia_cardiaca", IntegerType(), True),
        StructField("pressao_sistolica", IntegerType(), True),
        StructField("pressao_diastolica", IntegerType(), True),
        StructField("saturacao_o2", DoubleType(), True),
        StructField("temperatura", DoubleType(), True),
        StructField("frequencia_respiratoria", IntegerType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Leitura com Structured Streaming (Auto Loader)
# MAGIC
# MAGIC O Auto Loader do Databricks detecta automaticamente novos arquivos JSON
# MAGIC que chegam no S3 e os processa incrementalmente.

# COMMAND ----------

df_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema/")
    .option("cloudFiles.inferColumnTypes", "true")
    .schema(sinais_vitais_schema)
    .load(RAW_PATH)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Transformações em Streaming
# MAGIC
# MAGIC Adicionamos metadados e calculamos flags de alerta para sinais anormais.

# COMMAND ----------

df_enriched = (
    df_stream.withColumn("_ingestao_timestamp", F.current_timestamp())
    .withColumn("_notebook", F.lit("02_streaming_simulado"))
    .withColumn("data_particao", F.to_date("timestamp"))
    .withColumn(
        "alerta_fc",
        F.when(
            (F.col("frequencia_cardiaca") > 120)
            | (F.col("frequencia_cardiaca") < 50),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )
    .withColumn(
        "alerta_pa",
        F.when(
            (F.col("pressao_sistolica") > 180) | (F.col("pressao_sistolica") < 80),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )
    .withColumn(
        "alerta_spo2",
        F.when(F.col("saturacao_o2") < 92, F.lit(True)).otherwise(F.lit(False)),
    )
    .withColumn(
        "alerta_temp",
        F.when(
            (F.col("temperatura") > 38.5) | (F.col("temperatura") < 35.0),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )
    .withColumn(
        "alerta_critico",
        F.col("alerta_fc") | F.col("alerta_pa") | F.col("alerta_spo2"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Escrita na Camada Bronze (Delta Lake Streaming)

# COMMAND ----------

query = (
    df_enriched.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("data_particao")
    .trigger(availableNow=True)  # Processa todos os dados disponíveis e para
    .start(BRONZE_PATH)
)

query.awaitTermination()

print("Streaming batch concluído.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verificação dos Dados Ingeridos

# COMMAND ----------

df_check = spark.read.format("delta").load(BRONZE_PATH)

print(f"Total de registros na Bronze: {df_check.count()}")
print(f"Alertas críticos: {df_check.filter(F.col('alerta_critico') == True).count()}")
print(f"Pacientes distintos: {df_check.select('paciente_id').distinct().count()}")
print(f"Período: {df_check.agg(F.min('timestamp'), F.max('timestamp')).collect()[0]}")

display(df_check.filter(F.col("alerta_critico")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Agregações por Janela de Tempo (Demonstração)
# MAGIC
# MAGIC Para demonstrar capacidade de processamento de séries temporais,
# MAGIC calculamos métricas por janelas de 5 minutos.

# COMMAND ----------

df_windowed = (
    df_check.groupBy(
        F.window("timestamp", "5 minutes"), "paciente_id", "leito", "unidade_id"
    )
    .agg(
        F.avg("frequencia_cardiaca").alias("fc_media"),
        F.min("frequencia_cardiaca").alias("fc_min"),
        F.max("frequencia_cardiaca").alias("fc_max"),
        F.avg("pressao_sistolica").alias("pa_sis_media"),
        F.avg("saturacao_o2").alias("spo2_media"),
        F.avg("temperatura").alias("temp_media"),
        F.count("*").alias("num_leituras"),
        F.sum(F.col("alerta_critico").cast("int")).alias("num_alertas"),
    )
    .orderBy("window.start", "paciente_id")
)

print("Métricas por janela de 5 minutos:")
display(df_windowed.limit(20))
