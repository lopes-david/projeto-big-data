# Databricks notebook source
# MAGIC %md
# MAGIC # TP3 — 1.3 CDC com APPLY CHANGES INTO (DLT)
# MAGIC
# MAGIC **Objetivo:** Utilizar `APPLY CHANGES INTO` do Delta Live Tables para realizar
# MAGIC **Merge contínuo** (CDC — Change Data Capture) das tabelas de cadastro da
# MAGIC Camada Bronze para tabelas Silver atualizadas automaticamente.
# MAGIC
# MAGIC **Tabelas de cadastro processadas via CDC:**
# MAGIC | Bronze (fonte) | Silver CDC (destino) | Chave | Sequência |
# MAGIC |----------------|---------------------|-------|-----------|
# MAGIC | `bronze.customers` | `silver_customers_atualizada` | `customer_id` | `_ingestao_ts` |
# MAGIC | `bronze.sellers` | `silver_sellers_atualizada` | `seller_id` | `_ingestao_ts` |
# MAGIC | `bronze.products` | `silver_products_atualizada` | `product_id` | `_ingestao_ts` |
# MAGIC | `bronze.category_translation` | `silver_category_translation_atualizada` | `product_category_name` | `_ingestao_ts` |
# MAGIC
# MAGIC **Como funciona:**
# MAGIC 1. `dlt.read_stream()` lê incrementalmente da Bronze (Delta como streaming source)
# MAGIC 2. `dlt.create_streaming_table()` define a tabela destino Silver
# MAGIC 3. `dlt.apply_changes()` aplica MERGE contínuo: INSERT novos, UPDATE existentes
# MAGIC
# MAGIC **Configuração do pipeline (Databricks UI):**
# MAGIC - Pipeline name: `pb-brasilmart-silver-cdc`
# MAGIC - Target catalog: `pb_brasilmart`
# MAGIC - Target schema: `silver`
# MAGIC - Source: este notebook
# MAGIC - Pipeline mode: `Triggered` (ou `Continuous` para real-time)

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. CDC — Customers (Clientes)
# MAGIC
# MAGIC Chave: `customer_id` | Sequência: `_ingestao_ts`
# MAGIC
# MAGIC Transformações aplicadas no streaming:
# MAGIC - CEP formatado (5 dígitos), cidade lower, estado upper

# COMMAND ----------

@dlt.view(
    name="customers_bronze_stream",
    comment="Stream incremental da bronze.customers com limpeza aplicada"
)
def customers_bronze_stream():
    return (
        spark.readStream.table("pb_brasilmart.bronze.customers")
        .select(
            F.col("customer_id"),
            F.col("customer_unique_id"),
            F.lpad(F.col("customer_zip_code_prefix").cast("string"), 5, "0").alias("customer_zip_code"),
            F.lower(F.trim(F.col("customer_city"))).alias("customer_city"),
            F.upper(F.trim(F.col("customer_state"))).alias("customer_state"),
            F.col("_ingestao_ts"),
            F.col("_fonte"),
            F.col("_versao_ingestao")
        )
    )

# COMMAND ----------

dlt.create_streaming_table(
    name="silver_customers_atualizada",
    comment="Cadastro de clientes atualizado via CDC (APPLY CHANGES) — merge contínuo da Bronze",
    table_properties={"quality": "silver", "pipelines.metastore.tableName": "silver_customers_atualizada"}
)

dlt.apply_changes(
    target="silver_customers_atualizada",
    source="customers_bronze_stream",
    keys=["customer_id"],
    sequence_by=F.col("_ingestao_ts"),
    stored_as_scd_type=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. CDC — Sellers (Vendedores)
# MAGIC
# MAGIC Chave: `seller_id` | Sequência: `_ingestao_ts`
# MAGIC
# MAGIC Transformações: CEP formatado, cidade lower, estado upper

# COMMAND ----------

@dlt.view(
    name="sellers_bronze_stream",
    comment="Stream incremental da bronze.sellers com limpeza aplicada"
)
def sellers_bronze_stream():
    return (
        spark.readStream.table("pb_brasilmart.bronze.sellers")
        .select(
            F.col("seller_id"),
            F.lpad(F.col("seller_zip_code_prefix").cast("string"), 5, "0").alias("seller_zip_code"),
            F.lower(F.trim(F.col("seller_city"))).alias("seller_city"),
            F.upper(F.trim(F.col("seller_state"))).alias("seller_state"),
            F.col("_ingestao_ts"),
            F.col("_fonte"),
            F.col("_versao_ingestao")
        )
    )

# COMMAND ----------

dlt.create_streaming_table(
    name="silver_sellers_atualizada",
    comment="Cadastro de vendedores atualizado via CDC (APPLY CHANGES) — merge contínuo da Bronze",
    table_properties={"quality": "silver", "pipelines.metastore.tableName": "silver_sellers_atualizada"}
)

dlt.apply_changes(
    target="silver_sellers_atualizada",
    source="sellers_bronze_stream",
    keys=["seller_id"],
    sequence_by=F.col("_ingestao_ts"),
    stored_as_scd_type=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. CDC — Products (Produtos)
# MAGIC
# MAGIC Chave: `product_id` | Sequência: `_ingestao_ts`
# MAGIC
# MAGIC Transformações: categoria traduzida (join com category_translation),
# MAGIC peso em kg, volume cm³, classificação de porte

# COMMAND ----------

@dlt.view(
    name="products_bronze_stream",
    comment="Stream incremental da bronze.products com categoria traduzida e dimensões calculadas"
)
def products_bronze_stream():
    df_translation = spark.table("pb_brasilmart.bronze.category_translation")

    length_cm = F.col("p.product_length_cm").cast("decimal(10,2)")
    height_cm = F.col("p.product_height_cm").cast("decimal(10,2)")
    width_cm  = F.col("p.product_width_cm").cast("decimal(10,2)")
    weight_g  = F.col("p.product_weight_g").cast("decimal(10,2)")

    return (
        spark.readStream.table("pb_brasilmart.bronze.products").alias("p")
        .join(
            F.broadcast(df_translation).alias("t"),
            F.col("p.product_category_name") == F.col("t.product_category_name"),
            "left"
        )
        .select(
            F.col("p.product_id"),
            F.coalesce(
                F.col("t.product_category_name_english"),
                F.regexp_replace(F.col("p.product_category_name"), "_", " ")
            ).alias("product_category"),
            F.col("p.product_name_lenght").alias("product_name_length"),
            F.col("p.product_description_lenght").alias("product_description_length"),
            F.col("p.product_photos_qty").cast("int").alias("product_photos_qty"),
            F.round(weight_g / 1000, 2).alias("product_weight_kg"),
            F.round(length_cm, 1).alias("product_length_cm"),
            F.round(height_cm, 1).alias("product_height_cm"),
            F.round(width_cm, 1).alias("product_width_cm"),
            F.round(length_cm * height_cm * width_cm, 1).alias("product_volume_cm3"),
            F.when(weight_g <= 500, F.lit("pequeno"))
             .when(weight_g <= 5000, F.lit("medio"))
             .otherwise(F.lit("grande")).alias("porte_produto"),
            F.col("p._ingestao_ts"),
            F.col("p._fonte"),
            F.col("p._versao_ingestao")
        )
    )

# COMMAND ----------

dlt.create_streaming_table(
    name="silver_products_atualizada",
    comment="Catálogo de produtos atualizado via CDC (APPLY CHANGES) — merge contínuo da Bronze",
    table_properties={"quality": "silver", "pipelines.metastore.tableName": "silver_products_atualizada"}
)

dlt.apply_changes(
    target="silver_products_atualizada",
    source="products_bronze_stream",
    keys=["product_id"],
    sequence_by=F.col("_ingestao_ts"),
    stored_as_scd_type=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. CDC — Category Translation (Tradução de Categorias)
# MAGIC
# MAGIC Chave: `product_category_name` | Sequência: `_ingestao_ts`
# MAGIC
# MAGIC Transformações: trim nos nomes

# COMMAND ----------

@dlt.view(
    name="category_translation_bronze_stream",
    comment="Stream incremental da bronze.category_translation com nomes limpos"
)
def category_translation_bronze_stream():
    return (
        spark.readStream.table("pb_brasilmart.bronze.category_translation")
        .select(
            F.trim(F.col("product_category_name")).alias("product_category_name"),
            F.trim(F.col("product_category_name_english")).alias("product_category_name_english"),
            F.col("_ingestao_ts"),
            F.col("_fonte"),
            F.col("_versao_ingestao")
        )
    )

# COMMAND ----------

dlt.create_streaming_table(
    name="silver_category_translation_atualizada",
    comment="Tradução de categorias atualizada via CDC (APPLY CHANGES) — merge contínuo da Bronze",
    table_properties={"quality": "silver", "pipelines.metastore.tableName": "silver_category_translation_atualizada"}
)

dlt.apply_changes(
    target="silver_category_translation_atualizada",
    source="category_translation_bronze_stream",
    keys=["product_category_name"],
    sequence_by=F.col("_ingestao_ts"),
    stored_as_scd_type=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumo — CDC com APPLY CHANGES INTO
# MAGIC
# MAGIC ### Fluxo CDC
# MAGIC ```
# MAGIC bronze.customers ──stream──→ customers_bronze_stream ──APPLY CHANGES──→ silver_customers_atualizada
# MAGIC bronze.sellers   ──stream──→ sellers_bronze_stream   ──APPLY CHANGES──→ silver_sellers_atualizada
# MAGIC bronze.products  ──stream──→ products_bronze_stream  ──APPLY CHANGES──→ silver_products_atualizada
# MAGIC bronze.category  ──stream──→ category_bronze_stream  ──APPLY CHANGES──→ silver_category_translation_atualizada
# MAGIC ```
# MAGIC
# MAGIC ### Detalhes técnicos
# MAGIC | Aspecto | Implementação |
# MAGIC |---------|--------------|
# MAGIC | **Fonte** | `spark.readStream.table()` — leitura incremental da Bronze (Delta como streaming source) |
# MAGIC | **View intermediária** | `@dlt.view` — aplica limpeza/enriquecimento antes do merge |
# MAGIC | **Destino** | `dlt.create_streaming_table()` — tabela Silver CDC |
# MAGIC | **Merge** | `dlt.apply_changes()` — MERGE contínuo baseado em chave primária |
# MAGIC | **Ordenação** | `sequence_by=_ingestao_ts` — garante que a versão mais recente prevalece |
# MAGIC | **SCD Type** | Type 1 (sobrescreve) — mantém apenas a versão mais atual do cadastro |
# MAGIC
# MAGIC ### Por que CDC para tabelas de cadastro?
# MAGIC - **Cadastros mudam**: clientes trocam de endereço, vendedores atualizam dados, produtos são revisados
# MAGIC - **MERGE contínuo**: cada nova ingestão na Bronze é automaticamente propagada para a Silver
# MAGIC - **Idempotência**: `sequence_by` garante que reingestões não corrompem — sempre vence a mais recente
# MAGIC - **Eficiência**: `readStream` processa apenas dados novos (incremental), sem reprocessar tudo
# MAGIC
# MAGIC ### Diferença entre tabelas Silver base vs CDC
# MAGIC | Silver base (TP3 1.1/1.2) | Silver CDC (TP3 1.3) |
# MAGIC |---------------------------|----------------------|
# MAGIC | `@dlt.table` — reprocessa tudo (batch completo) | `apply_changes` — merge incremental (streaming) |
# MAGIC | Ideal para tabelas de fatos/eventos | Ideal para tabelas de cadastro/dimensão |
# MAGIC | `silver.orders`, `silver.items`, etc. | `silver.silver_customers_atualizada`, etc. |
# MAGIC
# MAGIC ### Como executar no Databricks
# MAGIC
# MAGIC 1. **Workflows → Delta Live Tables → Create Pipeline**
# MAGIC 2. Pipeline name: `pb-brasilmart-silver-cdc`
# MAGIC 3. Source code: este notebook
# MAGIC 4. Target catalog: `pb_brasilmart`
# MAGIC 5. Target schema: `silver`
# MAGIC 6. Pipeline mode: **Triggered** (execução sob demanda) ou **Continuous** (real-time)
# MAGIC 7. **Start** → o pipeline cria as 4 tabelas CDC Silver automaticamente
# MAGIC
# MAGIC > **Nota:** Este notebook pode ser adicionado ao mesmo pipeline `pb-brasilmart-silver`
# MAGIC > como segundo notebook source, ou executado como pipeline separado.
