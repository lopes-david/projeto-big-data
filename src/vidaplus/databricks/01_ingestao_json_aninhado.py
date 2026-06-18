# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 01 — Ingestão de JSON Aninhado (Exames de Laboratório)
# MAGIC
# MAGIC **Objetivo:** Ingerir dados complexos de exames laboratoriais em formato JSON com
# MAGIC múltiplos níveis de aninhamento, explodir as estruturas aninhadas e persistir
# MAGIC na camada Bronze em formato Delta Lake.
# MAGIC
# MAGIC **Fonte de dados:** `s3://vidaplus-raw-dev/exames_laboratorio/`
# MAGIC **Destino:** `s3://vidaplus-bronze-dev/exames_laboratorio/`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

RAW_PATH = "s3://vidaplus-raw-dev/exames_laboratorio/"
BRONZE_PATH = "s3://vidaplus-bronze-dev/exames_laboratorio/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Definição do Schema (JSON Aninhado)
# MAGIC
# MAGIC O JSON de exames laboratoriais possui 4 níveis de aninhamento:
# MAGIC ```
# MAGIC raiz → ordem_exame → paineis[] → resultados[] → referencia{}
# MAGIC ```

# COMMAND ----------

referencia_schema = StructType(
    [
        StructField("min", DoubleType(), True),
        StructField("max", DoubleType(), True),
        StructField("unidade", StringType(), True),
    ]
)

resultado_schema = StructType(
    [
        StructField("analito", StringType(), False),
        StructField("valor", DoubleType(), True),
        StructField("unidade", StringType(), True),
        StructField("referencia", referencia_schema, True),
        StructField("flags", ArrayType(StringType()), True),
    ]
)

painel_schema = StructType(
    [
        StructField("nome", StringType(), False),
        StructField("codigo_tuss", StringType(), True),
        StructField("resultados", ArrayType(resultado_schema), True),
    ]
)

medico_schema = StructType(
    [
        StructField("crm", StringType(), True),
        StructField("nome", StringType(), True),
        StructField("especialidade", StringType(), True),
    ]
)

ordem_schema = StructType(
    [
        StructField("ordem_id", StringType(), False),
        StructField("data_coleta", DateType(), True),
        StructField("data_resultado", DateType(), True),
        StructField("medico_solicitante", medico_schema, True),
        StructField("paineis", ArrayType(painel_schema), True),
        StructField("status", StringType(), True),
        StructField("urgente", StringType(), True),
    ]
)

exame_schema = StructType(
    [
        StructField("paciente_id", StringType(), False),
        StructField("unidade_id", StringType(), True),
        StructField("ordem_exame", ordem_schema, True),
        StructField("created_at", TimestampType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Leitura do JSON Aninhado

# COMMAND ----------

df_raw = spark.read.schema(exame_schema).option("multiLine", True).json(RAW_PATH)

print(f"Registros lidos: {df_raw.count()}")
print("Schema:")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Explosão das Estruturas Aninhadas
# MAGIC
# MAGIC Flatten do JSON em 3 etapas:
# MAGIC 1. Extrair campos do nível `ordem_exame`
# MAGIC 2. Explodir o array `paineis`
# MAGIC 3. Explodir o array `resultados` dentro de cada painel

# COMMAND ----------

# Etapa 1: Extrair campos da ordem de exame
df_ordem = df_raw.select(
    F.col("paciente_id"),
    F.col("unidade_id"),
    F.col("ordem_exame.ordem_id").alias("ordem_id"),
    F.col("ordem_exame.data_coleta").alias("data_coleta"),
    F.col("ordem_exame.data_resultado").alias("data_resultado"),
    F.col("ordem_exame.medico_solicitante.crm").alias("medico_crm"),
    F.col("ordem_exame.medico_solicitante.nome").alias("medico_nome"),
    F.col("ordem_exame.medico_solicitante.especialidade").alias(
        "medico_especialidade"
    ),
    F.col("ordem_exame.status").alias("ordem_status"),
    F.col("ordem_exame.urgente").alias("urgente"),
    F.col("ordem_exame.paineis").alias("paineis"),
    F.col("created_at"),
)

print(f"Após extração da ordem: {df_ordem.count()} registros")

# COMMAND ----------

# Etapa 2: Explodir painéis (1 ordem → N painéis)
df_paineis = df_ordem.select(
    "*", F.explode_outer("paineis").alias("painel")
).drop("paineis")

df_paineis = df_paineis.select(
    "*",
    F.col("painel.nome").alias("painel_nome"),
    F.col("painel.codigo_tuss").alias("painel_codigo_tuss"),
    F.col("painel.resultados").alias("resultados"),
).drop("painel")

print(f"Após explosão de painéis: {df_paineis.count()} registros")

# COMMAND ----------

# Etapa 3: Explodir resultados (1 painel → N analitos)
df_resultados = df_paineis.select(
    "*", F.explode_outer("resultados").alias("resultado")
).drop("resultados")

df_flat = df_resultados.select(
    "paciente_id",
    "unidade_id",
    "ordem_id",
    "data_coleta",
    "data_resultado",
    "medico_crm",
    "medico_nome",
    "medico_especialidade",
    "ordem_status",
    "urgente",
    "painel_nome",
    "painel_codigo_tuss",
    F.col("resultado.analito").alias("analito"),
    F.col("resultado.valor").alias("valor"),
    F.col("resultado.unidade").alias("valor_unidade"),
    F.col("resultado.referencia.min").alias("ref_min"),
    F.col("resultado.referencia.max").alias("ref_max"),
    F.col("resultado.flags").alias("flags"),
    "created_at",
).drop("resultado")

print(f"Após explosão completa (flat): {df_flat.count()} registros")
df_flat.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Adicionar Metadados de Ingestão

# COMMAND ----------

df_bronze = df_flat.withColumn(
    "_ingestao_timestamp", F.current_timestamp()
).withColumn("_notebook", F.lit("01_ingestao_json_aninhado"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Persistir na Camada Bronze (Delta Lake)

# COMMAND ----------

(
    df_bronze.write.format("delta")
    .mode("overwrite")
    .partitionBy("data_coleta")
    .save(BRONZE_PATH)
)

print(f"Dados salvos na camada Bronze: {BRONZE_PATH}")
print(f"Total de registros: {df_bronze.count()}")
print(f"Partições (datas): {df_bronze.select('data_coleta').distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verificação

# COMMAND ----------

df_check = spark.read.format("delta").load(BRONZE_PATH)
print(f"Registros na Bronze: {df_check.count()}")
display(df_check.limit(10))
