# Databricks notebook source
# MAGIC %md
# MAGIC # TP2 — 1.4 Upsert (MERGE) e Time Travel
# MAGIC
# MAGIC **Objetivo:** Demonstrar operações de MERGE (insert + update) no Delta Lake
# MAGIC e usar Time Travel para recuperar dados de versões anteriores.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG pb_brasilmart;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Estado Atual — Antes do MERGE

# COMMAND ----------

df_sellers = spark.table("bronze.sellers")
print(f"Total de sellers: {df_sellers.count()}")
display(df_sellers.limit(5))

# COMMAND ----------

# Salva contagem original pra comparar depois
contagem_original = df_sellers.count()
print(f"Versão atual — {contagem_original} sellers")

# Pega um seller existente pra demonstrar UPDATE
seller_existente = df_sellers.select("seller_id", "seller_city", "seller_state").first()
print(f"\nSeller existente: {seller_existente.seller_id}")
print(f"  Cidade atual: {seller_existente.seller_city}")
print(f"  Estado atual: {seller_existente.seller_state}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Preparar Dados para o MERGE
# MAGIC
# MAGIC Simulamos uma atualização vinda do sistema fonte:
# MAGIC - **2 sellers existentes** com dados atualizados (UPDATE)
# MAGIC - **2 sellers novos** que não existiam (INSERT)

# COMMAND ----------

# Pega 2 sellers existentes
sellers_existentes = df_sellers.limit(2).select("seller_id").collect()
id1 = sellers_existentes[0].seller_id
id2 = sellers_existentes[1].seller_id

# Dados de merge: 2 updates + 2 inserts
merge_data = [
    # UPDATE — sellers existentes com cidade alterada
    (id1, "00001000", "São Paulo", "SP", F.current_timestamp(), "merge_batch", "2026-06-18_merge"),
    (id2, "20000000", "Rio de Janeiro", "RJ", F.current_timestamp(), "merge_batch", "2026-06-18_merge"),
    # INSERT — sellers novos
    ("NOVO_SELLER_001", "90000000", "Porto Alegre", "RS", F.current_timestamp(), "merge_batch", "2026-06-18_merge"),
    ("NOVO_SELLER_002", "40000000", "Salvador", "BA", F.current_timestamp(), "merge_batch", "2026-06-18_merge"),
]

columns = ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state",
           "_ingestao_ts", "_fonte", "_versao_ingestao"]

df_updates = spark.createDataFrame(merge_data, columns)
print("Dados para MERGE:")
display(df_updates)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Executar o MERGE (Upsert)
# MAGIC
# MAGIC - Se `seller_id` já existe → **UPDATE** (atualiza cidade, estado, zip)
# MAGIC - Se `seller_id` não existe → **INSERT** (cria novo registro)

# COMMAND ----------

dt_sellers = DeltaTable.forName(spark, "pb_brasilmart.bronze.sellers")

(dt_sellers.alias("target")
 .merge(
     df_updates.alias("source"),
     "target.seller_id = source.seller_id"
 )
 .whenMatchedUpdate(set={
     "seller_zip_code_prefix": "source.seller_zip_code_prefix",
     "seller_city":            "source.seller_city",
     "seller_state":           "source.seller_state",
     "_ingestao_ts":           "source._ingestao_ts",
     "_fonte":                 "source._fonte",
     "_versao_ingestao":       "source._versao_ingestao",
 })
 .whenNotMatchedInsertAll()
 .execute()
)

print("✅ MERGE executado!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificar Resultado do MERGE

# COMMAND ----------

df_after = spark.table("bronze.sellers")
contagem_nova = df_after.count()

print(f"Antes do MERGE:  {contagem_original} sellers")
print(f"Depois do MERGE: {contagem_nova} sellers")
print(f"Novos inseridos: {contagem_nova - contagem_original}")

# COMMAND ----------

# Verifica os sellers atualizados
print("Sellers ATUALIZADOS (UPDATE):")
display(df_after.where(F.col("seller_id").isin(id1, id2)))

# COMMAND ----------

# Verifica os sellers novos
print("Sellers NOVOS (INSERT):")
display(df_after.where(F.col("seller_id").startswith("NOVO_SELLER")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Histórico de Versões (Delta Log)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY pb_brasilmart.bronze.sellers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Time Travel — Recuperar Dados Antigos
# MAGIC
# MAGIC O Delta Lake guarda todas as versões. Podemos acessar qualquer versão anterior
# MAGIC usando `VERSION AS OF` ou `TIMESTAMP AS OF`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Comparar versão anterior vs atual

# COMMAND ----------

# Versão anterior ao MERGE (versão 0 ou a penúltima)
history = spark.sql("DESCRIBE HISTORY pb_brasilmart.bronze.sellers").collect()
versao_antes_merge = history[1].version  # penúltima operação
versao_merge = history[0].version        # última operação (MERGE)

print(f"Versão antes do MERGE: {versao_antes_merge}")
print(f"Versão do MERGE:       {versao_merge}")

# COMMAND ----------

# Dados ANTES do merge via Time Travel
df_antes = spark.read.format("delta").option("versionAsOf", versao_antes_merge).table("pb_brasilmart.bronze.sellers")

# Dados DEPOIS do merge (versão atual)
df_depois = spark.table("bronze.sellers")

print(f"Versão {versao_antes_merge} (antes): {df_antes.count()} sellers")
print(f"Versão {versao_merge} (depois):  {df_depois.count()} sellers")

# COMMAND ----------

# O seller que foi ATUALIZADO — como era antes?
print(f"Seller {id1} ANTES do merge:")
display(df_antes.where(F.col("seller_id") == id1))

print(f"\nSeller {id1} DEPOIS do merge:")
display(df_depois.where(F.col("seller_id") == id1))

# COMMAND ----------

# Os sellers novos NÃO existiam na versão anterior
print("Sellers NOVO_SELLER na versão anterior (deve ser vazio):")
display(df_antes.where(F.col("seller_id").startswith("NOVO_SELLER")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Recuperar dados com SQL — VERSION AS OF

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consulta a versão anterior diretamente via SQL
# MAGIC SELECT seller_id, seller_city, seller_state, _versao_ingestao
# MAGIC FROM pb_brasilmart.bronze.sellers VERSION AS OF 0
# MAGIC WHERE seller_id IN (SELECT seller_id FROM pb_brasilmart.bronze.sellers WHERE seller_id LIKE 'NOVO_SELLER%')
# MAGIC    OR seller_city IN ('São Paulo', 'Rio de Janeiro');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Restaurar versão anterior (RESTORE)

# COMMAND ----------

# Conta antes de restaurar
print(f"Antes do RESTORE: {spark.table('bronze.sellers').count()} sellers")

# Restaura para a versão original
spark.sql(f"RESTORE TABLE pb_brasilmart.bronze.sellers TO VERSION AS OF {versao_antes_merge}")

print(f"Depois do RESTORE: {spark.table('bronze.sellers').count()} sellers")
print("\n✅ Tabela restaurada — sellers novos removidos, atualizações revertidas!")

# COMMAND ----------

# Confirma: sellers novos sumiram
display(spark.table("bronze.sellers").where(F.col("seller_id").startswith("NOVO_SELLER")))

# COMMAND ----------

# Histórico final — mostra todas as operações incluindo RESTORE
display(spark.sql("DESCRIBE HISTORY pb_brasilmart.bronze.sellers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo
# MAGIC
# MAGIC | Operação | O que fez |
# MAGIC |----------|-----------|
# MAGIC | **MERGE** | 2 sellers atualizados (UPDATE) + 2 novos inseridos (INSERT) |
# MAGIC | **Time Travel** | Acessou versão anterior e comparou dados antes/depois |
# MAGIC | **RESTORE** | Reverteu a tabela ao estado original, desfazendo o MERGE |
# MAGIC
# MAGIC O Delta Lake registra **cada operação como um commit atômico** no `_delta_log`.
# MAGIC Nenhum dado é perdido — todas as versões permanecem acessíveis para auditoria.
