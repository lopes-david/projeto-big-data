# Databricks notebook source
# MAGIC %md
# MAGIC # TP3 — 2.2 Validação da Camada Silver
# MAGIC
# MAGIC **Objetivo:** Notebook de validação executado pelo Databricks Workflow após o
# MAGIC pipeline DLT Silver. Verifica contagens, nulos e consistência entre tabelas.
# MAGIC
# MAGIC Faz parte do fluxo orquestrado:
# MAGIC `Executar_DLT → **Executar_Notebook_Validacao** → Otimizar_Tabela_Gold`

# COMMAND ----------

from pyspark.sql import functions as F
import json

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG pb_brasilmart;
# MAGIC USE SCHEMA silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificação de Contagens — Todas as tabelas Silver

# COMMAND ----------

SILVER_TABLES = [
    "orders", "customers", "items", "payments", "reviews",
    "products", "sellers", "geolocation", "category_translation",
    "orders_enriched", "items_enriched"
]

resultados = []
todas_ok = True

print(f"{'Tabela':<30} {'Registros':>10} {'Status':<10}")
print("-" * 55)

for t in SILVER_TABLES:
    try:
        df = spark.table(f"silver.{t}")
        count = df.count()
        status = "OK" if count > 0 else "VAZIA"
        if count == 0:
            todas_ok = False
        resultados.append({"tabela": t, "registros": count, "status": status})
        print(f"{t:<30} {count:>10,} {status:<10}")
    except Exception as e:
        todas_ok = False
        resultados.append({"tabela": t, "registros": 0, "status": "ERRO"})
        print(f"{t:<30} {'—':>10} {'ERRO':<10}  {str(e)[:60]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificação de Integridade Referencial

# COMMAND ----------

orders_ids = spark.table("silver.orders").select("order_id").distinct().count()
enriched_ids = spark.table("silver.orders_enriched").select("order_id").distinct().count()

print(f"orders:          {orders_ids:,} order_ids distintos")
print(f"orders_enriched: {enriched_ids:,} order_ids distintos")
print(f"Match:           {'OK' if orders_ids == enriched_ids else 'DIVERGENTE'}")

if orders_ids != enriched_ids:
    todas_ok = False

# COMMAND ----------

items_count = spark.table("silver.items").count()
items_enriched_count = spark.table("silver.items_enriched").count()

print(f"items:          {items_count:,} registros")
print(f"items_enriched: {items_enriched_count:,} registros")
print(f"Match:          {'OK' if items_count == items_enriched_count else 'DIVERGENTE'}")

if items_count != items_enriched_count:
    todas_ok = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificação de Nulos em Chaves Primárias

# COMMAND ----------

pk_checks = {
    "orders": "order_id",
    "customers": "customer_id",
    "items": "order_id",
    "payments": "order_id",
    "reviews": "review_id",
    "products": "product_id",
    "sellers": "seller_id",
}

print(f"{'Tabela':<20} {'PK':<20} {'Nulos':>8} {'Status':<10}")
print("-" * 62)

for table, pk in pk_checks.items():
    df = spark.table(f"silver.{table}")
    null_count = df.where(F.col(pk).isNull()).count()
    status = "OK" if null_count == 0 else "FALHA"
    if null_count > 0:
        todas_ok = False
    print(f"{table:<20} {pk:<20} {null_count:>8} {status:<10}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resultado Final da Validação

# COMMAND ----------

if todas_ok:
    print("VALIDACAO_SILVER_OK")
    dbutils.notebook.exit(json.dumps({"status": "OK", "tabelas": len(SILVER_TABLES)}))
else:
    print("VALIDACAO_SILVER_FALHA")
    dbutils.notebook.exit(json.dumps({"status": "FALHA", "detalhes": resultados}))
