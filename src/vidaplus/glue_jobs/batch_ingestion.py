"""
AWS Glue Job — Ingestão Batch de Consultas (CSV → Parquet)

Este job lê dados de consultas médicas em formato CSV da camada Raw,
aplica inferência de schema e salva em formato Parquet na camada Bronze.

Configuração no Glue Studio:
  - Tipo: Spark
  - Glue Version: 4.0
  - Workers: 2 (G.1X)
  - Job Bookmarks: Enabled (processamento incremental)
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH", "DATABASE_NAME", "TABLE_NAME"],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Schema esperado para consultas médicas
consultas_schema = StructType(
    [
        StructField("consulta_id", StringType(), False),
        StructField("paciente_id", StringType(), False),
        StructField("medico_id", StringType(), True),
        StructField("unidade_id", StringType(), True),
        StructField("especialidade", StringType(), True),
        StructField("data_agendamento", DateType(), True),
        StructField("data_consulta", DateType(), True),
        StructField("hora_consulta", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tipo_consulta", StringType(), True),
        StructField("convenio", StringType(), True),
        StructField("valor", StringType(), True),
        StructField("cid_principal", StringType(), True),
        StructField("tempo_espera_min", IntegerType(), True),
        StructField("no_show", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)

# Ler CSV da camada Raw
df_raw = (
    spark.read.option("header", "true")
    .option("delimiter", ";")
    .option("encoding", "UTF-8")
    .schema(consultas_schema)
    .csv(args["SOURCE_PATH"])
)

# Adicionar metadados de ingestão
df_bronze = df_raw.withColumn(
    "_ingestao_timestamp", F.current_timestamp()
).withColumn("_source_file", F.input_file_name())

# Salvar em Parquet na camada Bronze, particionado por data da consulta
df_bronze.write.mode("append").partitionBy("data_consulta").parquet(args["TARGET_PATH"])

# Registrar tabela no Glue Data Catalog
sink = glueContext.getSink(
    connection_type="s3",
    path=args["TARGET_PATH"],
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["data_consulta"],
)
sink.setCatalogInfo(
    catalogDatabase=args["DATABASE_NAME"], catalogTableName=args["TABLE_NAME"]
)
sink.setFormat("glueparquet")
sink.writeFrame(
    glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [args["TARGET_PATH"]]},
        format="parquet",
    )
)

job.commit()

print(f"Job concluído. Registros processados: {df_bronze.count()}")
