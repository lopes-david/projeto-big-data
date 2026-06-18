# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03 — Limpeza e Qualidade na Camada Bronze
# MAGIC
# MAGIC **Objetivo:** Aplicar limpeza básica nos dados da camada Bronze:
# MAGIC tratamento de nulos, remoção de duplicatas, validação de tipos,
# MAGIC filtros de qualidade e padronização.
# MAGIC
# MAGIC Este notebook processa os dados de:
# MAGIC - Exames laboratoriais (do Notebook 01)
# MAGIC - Sinais vitais IoT (do Notebook 02)
# MAGIC - Consultas médicas (do Glue Job batch)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

BRONZE_EXAMES = "s3://vidaplus-bronze-dev/exames_laboratorio/"
BRONZE_SINAIS = "s3://vidaplus-bronze-dev/sinais_vitais/"
BRONZE_CONSULTAS = "s3://vidaplus-bronze-dev/consultas/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Limpeza de Exames Laboratoriais

# COMMAND ----------

df_exames = spark.read.format("delta").load(BRONZE_EXAMES)

print(f"Exames — Registros antes da limpeza: {df_exames.count()}")

# 2.1 Análise de nulos
print("\nDistribuição de nulos por coluna:")
null_counts = df_exames.select(
    [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df_exames.columns]
)
display(null_counts)

# COMMAND ----------

# 2.2 Tratamento de nulos
df_exames_clean = (
    df_exames
    # Remover registros sem paciente_id ou ordem_id (campos obrigatórios)
    .filter(F.col("paciente_id").isNotNull() & F.col("ordem_id").isNotNull())
    # Preencher nulos em campos descritivos
    .fillna({"analito": "NAO_INFORMADO", "painel_nome": "NAO_INFORMADO"})
    # Preencher flags nulo com array vazio
    .withColumn(
        "flags", F.when(F.col("flags").isNull(), F.array()).otherwise(F.col("flags"))
    )
)

# COMMAND ----------

# 2.3 Remoção de duplicatas (mesmo exame registrado mais de uma vez)
window_dedup = Window.partitionBy("ordem_id", "analito").orderBy(
    F.col("_ingestao_timestamp").desc()
)

df_exames_clean = (
    df_exames_clean.withColumn("_row_num", F.row_number().over(window_dedup))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

# COMMAND ----------

# 2.4 Validação de valores de referência
df_exames_clean = df_exames_clean.withColumn(
    "valor_fora_referencia",
    F.when(
        (F.col("ref_min").isNotNull())
        & (F.col("ref_max").isNotNull())
        & ((F.col("valor") < F.col("ref_min")) | (F.col("valor") > F.col("ref_max"))),
        F.lit(True),
    ).otherwise(F.lit(False)),
)

# 2.5 Filtrar valores biologicamente impossíveis
df_exames_clean = df_exames_clean.filter(
    (F.col("valor").isNull()) | (F.col("valor") >= 0)
)

print(f"Exames — Registros após limpeza: {df_exames_clean.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Limpeza de Sinais Vitais

# COMMAND ----------

df_sinais = spark.read.format("delta").load(BRONZE_SINAIS)

print(f"Sinais Vitais — Registros antes da limpeza: {df_sinais.count()}")

# COMMAND ----------

# 3.1 Remover registros sem timestamp ou paciente_id
df_sinais_clean = df_sinais.filter(
    F.col("timestamp").isNotNull() & F.col("paciente_id").isNotNull()
)

# 3.2 Filtrar valores biologicamente impossíveis
df_sinais_clean = df_sinais_clean.filter(
    # Frequência cardíaca: 20-300 bpm (range amplo para incluir patologias)
    (
        F.col("frequencia_cardiaca").isNull()
        | F.col("frequencia_cardiaca").between(20, 300)
    )
    # Pressão sistólica: 40-300 mmHg
    & (
        F.col("pressao_sistolica").isNull()
        | F.col("pressao_sistolica").between(40, 300)
    )
    # SpO2: 50-100%
    & (F.col("saturacao_o2").isNull() | F.col("saturacao_o2").between(50, 100))
    # Temperatura: 30-45°C
    & (F.col("temperatura").isNull() | F.col("temperatura").between(30.0, 45.0))
)

# COMMAND ----------

# 3.3 Remoção de duplicatas de leituras (mesmo device, mesmo segundo)
window_sinais = Window.partitionBy("device_id", "timestamp").orderBy(
    F.col("_ingestao_timestamp").desc()
)

df_sinais_clean = (
    df_sinais_clean.withColumn("_row_num", F.row_number().over(window_sinais))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

# 3.4 Preencher nulos em sinais vitais com valor anterior (forward fill via window)
window_ffill = Window.partitionBy("paciente_id").orderBy("timestamp")

for col_name in [
    "frequencia_cardiaca",
    "pressao_sistolica",
    "pressao_diastolica",
    "saturacao_o2",
    "temperatura",
]:
    df_sinais_clean = df_sinais_clean.withColumn(
        col_name,
        F.when(F.col(col_name).isNull(), F.last(col_name, ignorenulls=True).over(window_ffill))
        .otherwise(F.col(col_name)),
    )

print(f"Sinais Vitais — Registros após limpeza: {df_sinais_clean.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Limpeza de Consultas

# COMMAND ----------

df_consultas = spark.read.parquet(BRONZE_CONSULTAS)

print(f"Consultas — Registros antes da limpeza: {df_consultas.count()}")

# COMMAND ----------

# 4.1 Remover registros sem IDs obrigatórios
df_consultas_clean = df_consultas.filter(
    F.col("consulta_id").isNotNull() & F.col("paciente_id").isNotNull()
)

# 4.2 Padronizar status
df_consultas_clean = df_consultas_clean.withColumn(
    "status",
    F.when(F.upper(F.col("status")).isin("REALIZADA", "CONCLUIDA", "CONCLUÍDA"), "REALIZADA")
    .when(F.upper(F.col("status")).isin("CANCELADA", "CANCELADO"), "CANCELADA")
    .when(F.upper(F.col("status")).isin("NO_SHOW", "NO-SHOW", "FALTA"), "NO_SHOW")
    .when(F.upper(F.col("status")).isin("AGENDADA", "CONFIRMADA"), "AGENDADA")
    .otherwise(F.upper(F.col("status"))),
)

# 4.3 Padronizar campo no_show como booleano
df_consultas_clean = df_consultas_clean.withColumn(
    "no_show",
    F.when(
        F.upper(F.col("no_show")).isin("SIM", "YES", "TRUE", "1", "S"), F.lit(True)
    ).otherwise(F.lit(False)),
)

# 4.4 Converter valor para double e tratar
df_consultas_clean = df_consultas_clean.withColumn(
    "valor",
    F.regexp_replace(F.col("valor"), "[R$\\s.]", ""),
).withColumn(
    "valor",
    F.regexp_replace(F.col("valor"), ",", ".").cast("double"),
)

# 4.5 Remoção de duplicatas
window_consultas = Window.partitionBy("consulta_id").orderBy(
    F.col("_ingestao_timestamp").desc()
)

df_consultas_clean = (
    df_consultas_clean.withColumn("_row_num", F.row_number().over(window_consultas))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

print(f"Consultas — Registros após limpeza: {df_consultas_clean.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Persistir Dados Limpos de Volta na Bronze
# MAGIC
# MAGIC Sobrescrevemos os dados na Bronze com a versão limpa.
# MAGIC O Delta Lake mantém o histórico via time travel.

# COMMAND ----------

df_exames_clean.write.format("delta").mode("overwrite").partitionBy(
    "data_coleta"
).save(BRONZE_EXAMES)

df_sinais_clean.write.format("delta").mode("overwrite").partitionBy(
    "data_particao"
).save(BRONZE_SINAIS)

df_consultas_clean.write.format("delta").mode("overwrite").partitionBy(
    "data_consulta"
).save(f"{BRONZE_CONSULTAS.rstrip('/')}_delta/")

print("Dados limpos persistidos na camada Bronze (Delta Lake).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Relatório de Qualidade

# COMMAND ----------

print("=" * 60)
print("RELATÓRIO DE QUALIDADE — CAMADA BRONZE")
print("=" * 60)

datasets = {
    "Exames Laboratoriais": (BRONZE_EXAMES, "delta"),
    "Sinais Vitais IoT": (BRONZE_SINAIS, "delta"),
}

for name, (path, fmt) in datasets.items():
    df = spark.read.format(fmt).load(path)
    total = df.count()
    cols = len(df.columns)

    null_pct = {}
    for c in df.columns:
        if not c.startswith("_"):
            n = df.filter(F.col(c).isNull()).count()
            if n > 0:
                null_pct[c] = round(n / total * 100, 2)

    print(f"\n{'─' * 60}")
    print(f"Dataset: {name}")
    print(f"  Registros: {total:,}")
    print(f"  Colunas: {cols}")
    if null_pct:
        print(f"  Colunas com nulos:")
        for c, pct in sorted(null_pct.items(), key=lambda x: x[1], reverse=True):
            print(f"    - {c}: {pct}%")
    else:
        print(f"  Nulos: nenhum")
    print(f"{'─' * 60}")
