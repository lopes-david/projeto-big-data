# Databricks notebook source
# MAGIC %md
# MAGIC # TP2 — 1.4 Upsert (MERGE) e Time Travel

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG olist_lakehouse;
# MAGIC USE SCHEMA tp2_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Estado Atual — Antes do MERGE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS total_customers FROM tp2_demo.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Amostra de 5 clientes
# MAGIC SELECT customer_id, customer_city, customer_state
# MAGIC FROM tp2_demo.customers LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Dados para o MERGE
# MAGIC
# MAGIC Simulamos uma atualização do sistema fonte:
# MAGIC - **2 clientes existentes** com cidade alterada (UPDATE)
# MAGIC - **2 clientes novos** que não existiam (INSERT)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW merge_source AS
# MAGIC (SELECT customer_id, customer_unique_id, '00001' AS customer_zip_code_prefix,
# MAGIC        'Curitiba' AS customer_city, 'PR' AS customer_state,
# MAGIC        _ingested_at, _source_file, _batch_id, current_timestamp() AS _ingestao_ts
# MAGIC FROM tp2_demo.customers WHERE customer_state = 'SP' LIMIT 1)
# MAGIC UNION ALL
# MAGIC (SELECT customer_id, customer_unique_id, '20000',
# MAGIC        'Niterói', 'RJ',
# MAGIC        _ingested_at, _source_file, _batch_id, current_timestamp()
# MAGIC FROM tp2_demo.customers WHERE customer_state = 'RJ' LIMIT 1)
# MAGIC UNION ALL
# MAGIC SELECT 'NOVO_CLI_001', 'UNIQUE_001', '90000', 'Porto Alegre', 'RS',
# MAGIC        NULL, NULL, NULL, current_timestamp()
# MAGIC UNION ALL
# MAGIC SELECT 'NOVO_CLI_002', 'UNIQUE_002', '40000', 'Salvador', 'BA',
# MAGIC        NULL, NULL, NULL, current_timestamp();

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM merge_source;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Executar o MERGE (Upsert)
# MAGIC
# MAGIC - `customer_id` já existe → **UPDATE**
# MAGIC - `customer_id` não existe → **INSERT**

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO tp2_demo.customers AS target
# MAGIC USING merge_source AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.customer_city = source.customer_city,
# MAGIC   target.customer_state = source.customer_state,
# MAGIC   target.customer_zip_code_prefix = source.customer_zip_code_prefix,
# MAGIC   target._ingestao_ts = source._ingestao_ts
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificar Resultado

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Contagem: deve ter +2 (os novos inseridos)
# MAGIC SELECT count(*) AS total_apos_merge FROM tp2_demo.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clientes NOVOS inseridos
# MAGIC SELECT * FROM tp2_demo.customers WHERE customer_id LIKE 'NOVO_CLI%';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clientes ATUALIZADOS (cidade mudou)
# MAGIC SELECT * FROM tp2_demo.customers WHERE customer_city IN ('Curitiba', 'Niterói');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Histórico — Commits no `_delta_log`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY tp2_demo.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Time Travel — Recuperar Dados Antigos

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Versão ANTES do merge (versão 0)
# MAGIC SELECT count(*) AS total_antes_merge FROM tp2_demo.customers VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Os clientes novos NÃO existiam na versão 0
# MAGIC SELECT * FROM tp2_demo.customers VERSION AS OF 0 WHERE customer_id LIKE 'NOVO_CLI%';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comparação: cidade do cliente ANTES vs DEPOIS do merge
# MAGIC SELECT
# MAGIC   'ANTES (v0)' AS versao,
# MAGIC   customer_id, customer_city, customer_state
# MAGIC FROM tp2_demo.customers VERSION AS OF 0
# MAGIC WHERE customer_city IN ('Curitiba', 'Niterói')
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'DEPOIS (atual)' AS versao,
# MAGIC   customer_id, customer_city, customer_state
# MAGIC FROM tp2_demo.customers
# MAGIC WHERE customer_city IN ('Curitiba', 'Niterói');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. RESTORE — Desfazer o MERGE

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE tp2_demo.customers TO VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS total_apos_restore FROM tp2_demo.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirma: clientes novos sumiram
# MAGIC SELECT * FROM tp2_demo.customers WHERE customer_id LIKE 'NOVO_CLI%';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Histórico final: CREATE → MERGE → RESTORE
# MAGIC DESCRIBE HISTORY tp2_demo.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo
# MAGIC
# MAGIC | Operação | Resultado |
# MAGIC |----------|----------|
# MAGIC | **MERGE** | 2 clientes atualizados + 2 novos inseridos |
# MAGIC | **Time Travel** | Versão 0 acessível mesmo após MERGE |
# MAGIC | **RESTORE** | Tabela revertida ao estado original |
