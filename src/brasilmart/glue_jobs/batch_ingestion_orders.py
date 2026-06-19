"""
AWS Glue Job — Ingestão Batch de Pedidos (olist_orders_dataset.csv → Parquet)

Lê o CSV de pedidos da Olist da camada Raw, aplica schema explícito,
adiciona metadados de ingestão e salva em Parquet particionado por
data de compra na camada Bronze.

Parâmetros (Job Arguments no Glue Studio):
  --SOURCE_PATH     s3://brasilmart-raw-dev/orders/olist_orders_dataset.csv
  --TARGET_PATH     s3://brasilmart-bronze-dev/orders/
  --DATABASE_NAME   brasilmart_bronze
  --TABLE_NAME      orders
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH", "DATABASE_NAME", "TABLE_NAME"],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Schema explícito do olist_orders_dataset.csv
orders_schema = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", TimestampType(), True),
        StructField("order_approved_at", TimestampType(), True),
        StructField("order_delivered_carrier_date", TimestampType(), True),
        StructField("order_delivered_customer_date", TimestampType(), True),
        StructField("order_estimated_delivery_date", TimestampType(), True),
    ]
)

# Ler CSV da camada Raw
df_raw = (
    spark.read.option("header", "true")
    .option("quote", '"')
    .schema(orders_schema)
    .csv(args["SOURCE_PATH"])
)

print(f"Registros lidos: {df_raw.count()}")

# Coluna de partição e metadados de ingestão
df_bronze = (
    df_raw.withColumn(
        "order_purchase_date",
        F.to_date("order_purchase_timestamp"),
    )
    .withColumn("_ingestao_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# Calcular delta_entrega (dias de atraso/antecedência vs estimativa)
df_bronze = df_bronze.withColumn(
    "delta_entrega_dias",
    F.datediff(
        F.col("order_delivered_customer_date"),
        F.col("order_estimated_delivery_date"),
    ),
)

# Salvar em Parquet, particionado por data de compra
df_bronze.write.mode("append").partitionBy("order_purchase_date").parquet(
    args["TARGET_PATH"]
)

# Registrar no Glue Data Catalog
sink = glueContext.getSink(
    connection_type="s3",
    path=args["TARGET_PATH"],
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["order_purchase_date"],
)
sink.setCatalogInfo(
    catalogDatabase=args["DATABASE_NAME"],
    catalogTableName=args["TABLE_NAME"],
)
sink.setFormat("glueparquet")

job.commit()

print(f"Parquet salvo em: {args['TARGET_PATH']}")
print(f"Total processado: {df_bronze.count()} registros")
