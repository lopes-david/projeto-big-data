# Databricks notebook source
# MAGIC %md
# MAGIC # TP2 — 1.1 Conversão para Delta Lake e Transacionalidade
# MAGIC
# MAGIC **Objetivo:** Converter dados para Delta Lake e evidenciar como o `_delta_log`
# MAGIC garante transacionalidade ACID.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG olist_lakehouse;
# MAGIC CREATE SCHEMA IF NOT EXISTS tp2_demo;
# MAGIC USE SCHEMA tp2_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Conversão dos Dados para Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tp2_demo.orders
# MAGIC USING DELTA
# MAGIC COMMENT '99k pedidos Olist — Delta Lake'
# MAGIC AS SELECT *, current_timestamp() AS _ingestao_ts FROM bronze.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tp2_demo.customers
# MAGIC USING DELTA
# MAGIC COMMENT '99k clientes Olist — Delta Lake'
# MAGIC AS SELECT *, current_timestamp() AS _ingestao_ts FROM bronze.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tp2_demo.products
# MAGIC USING DELTA
# MAGIC COMMENT '32k produtos Olist — Delta Lake'
# MAGIC AS SELECT *, current_timestamp() AS _ingestao_ts FROM bronze.products;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'orders' AS tabela, count(*) AS registros FROM tp2_demo.orders
# MAGIC UNION ALL
# MAGIC SELECT 'customers', count(*) FROM tp2_demo.customers
# MAGIC UNION ALL
# MAGIC SELECT 'products', count(*) FROM tp2_demo.products;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Evidência do `_delta_log` — DESCRIBE DETAIL e HISTORY
# MAGIC
# MAGIC O Delta Lake armazena um **log de transações** (`_delta_log/`).
# MAGIC Cada operação gera um commit JSON numerado sequencialmente.
# MAGIC
# MAGIC Isso garante **ACID**:
# MAGIC - **Atomicidade** — commit é tudo-ou-nada
# MAGIC - **Consistência** — schema validado antes de aceitar dados
# MAGIC - **Isolamento** — leitores veem apenas commits completos
# MAGIC - **Durabilidade** — log persiste junto com os dados

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mostra formato Delta, localização e número de arquivos
# MAGIC DESCRIBE DETAIL tp2_demo.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cada linha = 1 commit no _delta_log (1 arquivo JSON)
# MAGIC DESCRIBE HISTORY tp2_demo.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Time Travel — Acessar versões anteriores

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Versão 0: estado atual
# MAGIC SELECT count(*) AS total_v0 FROM tp2_demo.orders VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simula chegada de novos pedidos (APPEND)
# MAGIC INSERT INTO tp2_demo.orders
# MAGIC SELECT *, current_timestamp() AS _ingestao_ts
# MAGIC FROM bronze.orders LIMIT 200;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Versão 1: após append (+200 registros)
# MAGIC SELECT count(*) AS total_atual FROM tp2_demo.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Time Travel: volta pra versão 0 SEM perder a versão 1
# MAGIC SELECT count(*) AS total_time_travel_v0 FROM tp2_demo.orders VERSION AS OF 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Novos commits registrados no `_delta_log`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Agora mostra 2 commits: CREATE TABLE + INSERT
# MAGIC DESCRIBE HISTORY tp2_demo.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. RESTORE — Reverter ao estado original

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE tp2_demo.orders TO VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirma: voltou ao estado original
# MAGIC SELECT count(*) AS total_apos_restore FROM tp2_demo.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Histórico final: CREATE → INSERT → RESTORE (3 commits no _delta_log)
# MAGIC DESCRIBE HISTORY tp2_demo.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusão
# MAGIC
# MAGIC | Propriedade ACID | Como o `_delta_log` garante |
# MAGIC |-----------------|---------------------------|
# MAGIC | **Atomicidade** | Cada commit é um arquivo JSON completo — tudo-ou-nada |
# MAGIC | **Consistência** | Schema validado antes de aceitar novos dados |
# MAGIC | **Isolamento** | Leitores veem apenas commits finalizados |
# MAGIC | **Durabilidade** | Log persiste no storage junto com os Parquet |
# MAGIC
# MAGIC Demonstrado: conversão Delta, `DESCRIBE HISTORY` (evidência do _delta_log), Time Travel e RESTORE.
