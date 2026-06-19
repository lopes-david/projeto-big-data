"""Funções utilitárias para sessões Spark (Databricks / Local)."""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "BrasilMart") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )
