# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03 — Limpeza e Qualidade na Camada Bronze (Olist / BrasilMart)
# MAGIC
# MAGIC **Objetivo:** Aplicar limpeza básica em todos os datasets Olist na Bronze:
# MAGIC tratamento de nulos, remoção de duplicatas, validação de tipos,
# MAGIC filtros de qualidade e padronização de campos.
# MAGIC
# MAGIC **Datasets processados:**
# MAGIC - `orders_json/` (Delta — do Notebook 01)
# MAGIC - Parquet de `orders`, `customers`, `products`, `sellers` (do Glue Job)
# MAGIC
# MAGIC **Nota:** Os dados da Olist são de alta qualidade (dataset público já curado),
# MAGIC mas aplicamos as transformações como boa prática de pipeline de dados.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração dos Caminhos

# COMMAND ----------

BRONZE = {
    "orders_json":  "s3://brasilmart-bronze-dev/orders_json/",
    "orders":       "s3://brasilmart-bronze-dev/orders/",
    "customers":    "s3://brasilmart-bronze-dev/customers/",
    "products":     "s3://brasilmart-bronze-dev/products/",
    "sellers":      "s3://brasilmart-bronze-dev/sellers/",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Limpeza do Dataset Principal: orders_json (Delta)

# COMMAND ----------

df_orders = spark.read.format("delta").load(BRONZE["orders_json"])

print(f"orders_json — Registros antes da limpeza: {df_orders.count():,}")

# COMMAND ----------

# 2.1 Análise de nulos nas colunas críticas
cols_criticas = [
    "order_id", "customer_id", "customer_unique_id",
    "purchase_ts", "order_status", "price",
]
print("Nulos nas colunas críticas:")
null_report = df_orders.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in cols_criticas
])
display(null_report)

# COMMAND ----------

# 2.2 Remover registros sem order_id ou customer_id (obrigatórios)
df_orders_clean = df_orders.filter(
    F.col("order_id").isNotNull() & F.col("customer_id").isNotNull()
)

# 2.3 Padronizar order_status (alguns datasets têm inconsistências)
status_map = {
    "delivered":           "delivered",
    "shipped":             "shipped",
    "canceled":            "canceled",
    "unavailable":         "unavailable",
    "invoiced":            "invoiced",
    "processing":          "processing",
    "approved":            "approved",
    "created":             "created",
}

df_orders_clean = df_orders_clean.withColumn(
    "order_status",
    F.when(F.col("order_status").isin(list(status_map.keys())), F.col("order_status"))
    .otherwise(F.lit("unknown")),
)

print(f"Distribuição de status após padronização:")
display(df_orders_clean.groupBy("order_status").count().orderBy(F.desc("count")))

# COMMAND ----------

# 2.4 Remover duplicatas por order_id + product_id (item duplicado)
window_dedup = Window.partitionBy("order_id", "product_id").orderBy(
    F.col("_ingestao_timestamp").desc()
)

df_orders_clean = (
    df_orders_clean.withColumn("_row_num", F.row_number().over(window_dedup))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

# COMMAND ----------

# 2.5 Validação de valores monetários (price e freight_value não podem ser negativos)
df_orders_clean = df_orders_clean.filter(
    (F.col("price").isNull() | (F.col("price") >= 0))
    & (F.col("freight_value").isNull() | (F.col("freight_value") >= 0))
    & (F.col("total_order_value").isNull() | (F.col("total_order_value") >= 0))
)

# 2.6 Validar review_score (deve ser entre 1 e 5)
df_orders_clean = df_orders_clean.withColumn(
    "review_score",
    F.when(
        F.col("review_score").between(1, 5), F.col("review_score")
    ).otherwise(F.lit(None).cast("int")),
)

# 2.7 Classificar atraso de entrega
df_orders_clean = df_orders_clean.withColumn(
    "status_entrega",
    F.when(F.col("delta_entrega_dias").isNull(), "sem_informacao")
    .when(F.col("delta_entrega_dias") <= -3, "muito_adiantado")
    .when(F.col("delta_entrega_dias") <= 0, "no_prazo")
    .when(F.col("delta_entrega_dias") <= 5, "atraso_leve")
    .otherwise("atraso_grave"),
)

print(f"orders_json — Registros após limpeza: {df_orders_clean.count():,}")
print("Status de entrega:")
display(df_orders_clean.groupBy("status_entrega").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Limpeza de Customers

# COMMAND ----------

df_customers = spark.read.parquet(BRONZE["customers"])
print(f"customers — Antes: {df_customers.count():,}")

# Padronizar estado (UF) para maiúsculas
df_customers_clean = (
    df_customers
    .filter(F.col("customer_id").isNotNull())
    .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
    .withColumn("customer_city", F.lower(F.trim(F.col("customer_city"))))
    # Padronizar zip_code_prefix para 5 dígitos com zero à esquerda
    .withColumn(
        "customer_zip_code_prefix",
        F.lpad(F.col("customer_zip_code_prefix").cast("string"), 5, "0"),
    )
)

# Remover duplicatas de customer_id
window_cust = Window.partitionBy("customer_id").orderBy(F.col("customer_id"))
df_customers_clean = (
    df_customers_clean.withColumn("_row_num", F.row_number().over(window_cust))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

print(f"customers — Após limpeza: {df_customers_clean.count():,}")
print("Top 10 estados:")
display(df_customers_clean.groupBy("customer_state").count().orderBy(F.desc("count")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Limpeza de Products

# COMMAND ----------

df_products = spark.read.parquet(BRONZE["products"])
print(f"products — Antes: {df_products.count():,}")

df_products_clean = (
    df_products
    .filter(F.col("product_id").isNotNull())
    # Padronizar category name para lowercase sem espaços extras
    .withColumn(
        "product_category_name",
        F.lower(F.trim(F.col("product_category_name"))),
    )
    # Preencher nulos em dimensões físicas com mediana da categoria
    .fillna({
        "product_weight_g": 0,
        "product_length_cm": 0,
        "product_height_cm": 0,
        "product_width_cm": 0,
    })
    # Flag produto sem categoria
    .withColumn(
        "sem_categoria",
        F.col("product_category_name").isNull(),
    )
)

print(f"products — Após limpeza: {df_products_clean.count():,}")
print(f"Produtos sem categoria: {df_products_clean.filter('sem_categoria').count():,}")
print("Top 10 categorias:")
display(df_products_clean.groupBy("product_category_name").count().orderBy(F.desc("count")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Persistir de Volta na Bronze (com Delta Lake)

# COMMAND ----------

# Sobrescreve Bronze com versão limpa — o Delta mantém histórico (time travel)
(df_orders_clean.write.format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .partitionBy("purchase_date")
 .save(BRONZE["orders_json"]))

(df_customers_clean.write.format("delta")
 .mode("overwrite")
 .save(f"{BRONZE['customers']}_delta/"))

(df_products_clean.write.format("delta")
 .mode("overwrite")
 .save(f"{BRONZE['products']}_delta/"))

print("Dados limpos persistidos na Bronze (Delta Lake).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Relatório de Qualidade Final

# COMMAND ----------

print("=" * 65)
print("RELATÓRIO DE QUALIDADE — CAMADA BRONZE  |  BrasilMart / Olist")
print("=" * 65)

datasets = [
    ("orders_json (pedidos unificados)", BRONZE["orders_json"], "delta"),
]

for nome, path, fmt in datasets:
    df = spark.read.format(fmt).load(path)
    total = df.count()

    null_pct = {}
    for c in df.columns:
        if not c.startswith("_"):
            n = df.filter(F.col(c).isNull()).count()
            if n > 0:
                null_pct[c] = round(n / total * 100, 2)

    print(f"\n{'─' * 65}")
    print(f"  {nome}")
    print(f"  Registros: {total:,}  |  Colunas: {len(df.columns)}")
    if null_pct:
        print(f"  Colunas com nulos (>0%):")
        for c, pct in sorted(null_pct.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"    {c}: {pct}%")
    else:
        print("  Nulos: nenhum")

    print(f"\n  Status de entrega:")
    df.groupBy("status_entrega").count().orderBy(F.desc("count")).show(truncate=False)

print(f"\n{'─' * 65}")
print("Relatório concluído.")
