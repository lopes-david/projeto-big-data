# Databricks notebook source
# MAGIC %md
# MAGIC # TP2 — 1.2 Configuração do Unity Catalog
# MAGIC
# MAGIC **Objetivo:** Criar Catálogo, Schemas (databases) e Volumes para organizar
# MAGIC os dados do projeto BrasilMart dentro do Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Criar o Catálogo do Projeto

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS pb_brasilmart
# MAGIC COMMENT 'Catálogo do projeto BrasilMart — Visão 360° do Cliente (Olist Dataset)';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG pb_brasilmart;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criar os Schemas (um por camada do Lakehouse)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC COMMENT 'Dados brutos convertidos para Delta Lake — sem transformação';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC COMMENT 'Dados limpos, padronizados e dedupliados';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC COMMENT 'Tabelas analíticas prontas para consumo (RFM, KPIs, fatos)';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verifica os schemas criados
# MAGIC SHOW SCHEMAS IN pb_brasilmart;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Criar Volumes (armazenamento de arquivos não-tabulares)
# MAGIC
# MAGIC Volumes são usados para armazenar arquivos brutos (CSVs, JSONs, logs)
# MAGIC que ainda não foram convertidos em tabelas Delta.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS bronze.raw_files
# MAGIC COMMENT 'CSVs originais da Olist antes da conversão Delta';
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS silver.staging_files
# MAGIC COMMENT 'Arquivos intermediários do processamento Silver';
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS gold.export_files
# MAGIC COMMENT 'Exports e relatórios gerados a partir das tabelas Gold';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Registrar as tabelas Delta como Managed Tables

# COMMAND ----------

BRONZE_DELTA = "s3://pb-bronze-brasilmart-234828142988/delta"

TABLES = {
    "orders":     "Pedidos — 99.441 registros",
    "customers":  "Clientes — 99.441 registros",
    "items":      "Itens de pedido — detalhamento por produto",
    "payments":   "Pagamentos — parcelas e métodos",
    "reviews":    "Avaliações — score e comentários",
    "products":   "Produtos — 32.951 SKUs",
    "sellers":    "Vendedores — 3.095 sellers",
    "geolocation": "Geolocalização — 1M+ registros de CEP",
    "category_translation": "Tradução de categorias pt→en",
}

for table_name, description in TABLES.items():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS pb_brasilmart.bronze.{table_name}
        USING DELTA
        LOCATION '{BRONZE_DELTA}/{table_name}'
        COMMENT '{description}'
    """)
    count = spark.table(f"pb_brasilmart.bronze.{table_name}").count()
    print(f"  ✓ bronze.{table_name}: {count} registros")

print("\n✅ Todas as tabelas registradas no Unity Catalog!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificação — Estrutura completa do catálogo

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lista todas as tabelas do schema bronze
# MAGIC SHOW TABLES IN pb_brasilmart.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lista todos os volumes
# MAGIC SHOW VOLUMES IN pb_brasilmart.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Detalhes de uma tabela (schema, localização, formato)
# MAGIC DESCRIBE EXTENDED pb_brasilmart.bronze.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estrutura Final do Unity Catalog
# MAGIC
# MAGIC ```
# MAGIC pb_brasilmart (Catálogo)
# MAGIC ├── bronze (Schema)
# MAGIC │   ├── orders          (Delta Table)
# MAGIC │   ├── customers       (Delta Table)
# MAGIC │   ├── items           (Delta Table)
# MAGIC │   ├── payments        (Delta Table)
# MAGIC │   ├── reviews         (Delta Table)
# MAGIC │   ├── products        (Delta Table)
# MAGIC │   ├── sellers         (Delta Table)
# MAGIC │   ├── geolocation     (Delta Table)
# MAGIC │   ├── category_translation (Delta Table)
# MAGIC │   └── raw_files       (Volume)
# MAGIC ├── silver (Schema)
# MAGIC │   └── staging_files   (Volume)
# MAGIC └── gold (Schema)
# MAGIC     └── export_files    (Volume)
# MAGIC ```
