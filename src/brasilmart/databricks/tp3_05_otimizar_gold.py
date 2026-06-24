# Databricks notebook source
# MAGIC %md
# MAGIC # TP3 — 2.2 Otimização das Tabelas Gold
# MAGIC
# MAGIC **Objetivo:** Notebook executado pelo Databricks Workflow após a validação Silver.
# MAGIC Aplica OPTIMIZE e Z-ORDER nas tabelas Gold para performance de consultas analíticas.
# MAGIC
# MAGIC Faz parte do fluxo orquestrado:
# MAGIC `Executar_DLT → Executar_Notebook_Validacao → **Otimizar_Tabela_Gold**`

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG pb_brasilmart;
# MAGIC USE SCHEMA gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Listar Tabelas Gold Existentes

# COMMAND ----------

gold_tables = [row.tableName for row in spark.sql("SHOW TABLES IN pb_brasilmart.gold").collect()]
print(f"Tabelas Gold encontradas: {gold_tables}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. OPTIMIZE + Z-ORDER
# MAGIC
# MAGIC Aplica compactação de small files e Z-ORDER nas colunas mais usadas em filtros.

# COMMAND ----------

optimize_config = {
    "dim_clientes_rfm":         "customer_unique_id",
    "dim_sellers_score":        "seller_id",
    "dim_produtos_performance": "product_id",
    "fato_vendas_diarias":      "data_venda",
}

for table_name, zorder_col in optimize_config.items():
    if table_name in gold_tables:
        print(f"\nOTIMIZANDO: gold.{table_name} (Z-ORDER BY {zorder_col})")
        spark.sql(f"OPTIMIZE pb_brasilmart.gold.{table_name} ZORDER BY ({zorder_col})")
        print(f"  OK")
    else:
        print(f"\nSKIP: gold.{table_name} (tabela ainda não existe)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. VACUUM — Limpeza de Arquivos Obsoletos

# COMMAND ----------

for table_name in optimize_config:
    if table_name in gold_tables:
        print(f"VACUUM: gold.{table_name} (retention 168h / 7 dias)")
        spark.sql(f"VACUUM pb_brasilmart.gold.{table_name} RETAIN 168 HOURS")
        print(f"  OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Estatísticas Pós-Otimização

# COMMAND ----------

print(f"{'Tabela Gold':<30} {'Registros':>10} {'Versão':>8}")
print("-" * 52)

for table_name in optimize_config:
    if table_name in gold_tables:
        count = spark.table(f"gold.{table_name}").count()
        history = spark.sql(f"DESCRIBE HISTORY pb_brasilmart.gold.{table_name} LIMIT 1").collect()
        version = history[0].version if history else "?"
        print(f"{table_name:<30} {count:>10,} {version:>8}")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({"status": "OK", "tabelas_otimizadas": list(optimize_config.keys())}))
