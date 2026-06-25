# Databricks notebook source
# MAGIC %md
# MAGIC # TP2 — 1.5 OPTIMIZE e Z-ORDER

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG olist_lakehouse;
# MAGIC USE SCHEMA tp2_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Estado ANTES da otimização

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL tp2_demo.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL tp2_demo.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL tp2_demo.products;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. OPTIMIZE — Compactação de arquivos
# MAGIC
# MAGIC Combina arquivos pequenos em arquivos maiores, reduzindo overhead de I/O.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE tp2_demo.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE tp2_demo.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE tp2_demo.products;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Z-ORDER — Reorganização para consultas filtradas
# MAGIC
# MAGIC Coloca registros com valores próximos na mesma região do arquivo.
# MAGIC Consultas com `WHERE` nessas colunas pulam arquivos inteiros (data skipping).

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE tp2_demo.orders ZORDER BY (customer_id, order_status);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE tp2_demo.customers ZORDER BY (customer_state);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE tp2_demo.products ZORDER BY (product_category_name);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Estado DEPOIS da otimização

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL tp2_demo.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL tp2_demo.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Histórico — Operações OPTIMIZE registradas

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY tp2_demo.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. VACUUM — Limpeza de arquivos obsoletos

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM tp2_demo.orders DRY RUN;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo
# MAGIC
# MAGIC | Operação | O que faz | Quando usar |
# MAGIC |----------|-----------|-------------|
# MAGIC | **OPTIMIZE** | Compacta small files | Após muitas escritas |
# MAGIC | **Z-ORDER** | Reorganiza por coluna de filtro | Colunas usadas em WHERE |
# MAGIC | **VACUUM** | Remove arquivos obsoletos | Manutenção periódica |
