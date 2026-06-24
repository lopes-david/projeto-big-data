# Databricks notebook source
# MAGIC %md
# MAGIC # TP2 — 1.5 Otimização com OPTIMIZE e Z-ORDER
# MAGIC
# MAGIC **Objetivo:** Otimizar as tabelas Delta compactando arquivos pequenos (OPTIMIZE)
# MAGIC e reorganizando dados para acelerar consultas filtradas (Z-ORDER).

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG pb_brasilmart;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Diagnóstico — Estado ANTES da Otimização
# MAGIC
# MAGIC Tabelas Delta acumulam muitos arquivos pequenos (small files problem)
# MAGIC após múltiplas operações de escrita. Isso degrada a performance de leitura.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL pb_brasilmart.bronze.orders;

# COMMAND ----------

# Conta arquivos da tabela orders antes do OPTIMIZE
detail_before = spark.sql("DESCRIBE DETAIL pb_brasilmart.bronze.orders").first()
print(f"Tabela: bronze.orders")
print(f"  Formato: {detail_before.format}")
print(f"  Num arquivos: {detail_before.numFiles}")
print(f"  Tamanho total: {detail_before.sizeInBytes / 1024 / 1024:.2f} MB")

# COMMAND ----------

# Diagnóstico de todas as tabelas Bronze
print(f"{'Tabela':<25} {'Arquivos':>10} {'Tamanho (MB)':>15}")
print("-" * 55)

tables = ["orders", "customers", "items", "payments", "reviews",
          "products", "sellers", "geolocation", "category_translation"]

for t in tables:
    d = spark.sql(f"DESCRIBE DETAIL pb_brasilmart.bronze.{t}").first()
    size_mb = d.sizeInBytes / 1024 / 1024
    print(f"{t:<25} {d.numFiles:>10} {size_mb:>15.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. OPTIMIZE — Compactação de Small Files
# MAGIC
# MAGIC O `OPTIMIZE` combina arquivos pequenos em arquivos maiores (~1GB),
# MAGIC reduzindo o overhead de I/O e acelerando leituras.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE pb_brasilmart.bronze.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE pb_brasilmart.bronze.items;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE pb_brasilmart.bronze.payments;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE pb_brasilmart.bronze.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE pb_brasilmart.bronze.geolocation;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Z-ORDER — Reorganização para Consultas Filtradas
# MAGIC
# MAGIC O Z-ORDER coloca registros com valores próximos da coluna escolhida
# MAGIC nos mesmos arquivos. Consultas com `WHERE` nessa coluna pulam arquivos
# MAGIC inteiros (data skipping), lendo muito menos dados.
# MAGIC
# MAGIC **Escolha das colunas Z-ORDER** — baseada nos filtros mais comuns do negócio:
# MAGIC - `orders`: filtrado por `customer_id` (visão 360°) e `order_status`
# MAGIC - `items`: filtrado por `order_id` (detalhamento do pedido) e `product_id`
# MAGIC - `payments`: filtrado por `order_id`
# MAGIC - `geolocation`: filtrado por `geolocation_zip_code_prefix` (análise regional)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE pb_brasilmart.bronze.orders
# MAGIC ZORDER BY (customer_id, order_status);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE pb_brasilmart.bronze.items
# MAGIC ZORDER BY (order_id, product_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE pb_brasilmart.bronze.payments
# MAGIC ZORDER BY (order_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE pb_brasilmart.bronze.geolocation
# MAGIC ZORDER BY (geolocation_zip_code_prefix);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Diagnóstico — Estado DEPOIS da Otimização

# COMMAND ----------

print(f"{'Tabela':<25} {'Arquivos':>10} {'Tamanho (MB)':>15}")
print("-" * 55)

for t in tables:
    d = spark.sql(f"DESCRIBE DETAIL pb_brasilmart.bronze.{t}").first()
    size_mb = d.sizeInBytes / 1024 / 1024
    print(f"{t:<25} {d.numFiles:>10} {size_mb:>15.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Benchmark — Comparação de Performance

# COMMAND ----------

# Consulta SEM Z-ORDER (versão anterior via Time Travel)
history = spark.sql("DESCRIBE HISTORY pb_brasilmart.bronze.orders").collect()
versao_antes = [h.version for h in history if h.operation != "OPTIMIZE"][0]

# COMMAND ----------

import time

# Consulta na versão ANTES do Z-ORDER
start = time.time()
result_before = (spark.read.format("delta")
    .option("versionAsOf", versao_antes)
    .table("pb_brasilmart.bronze.orders")
    .where("order_status = 'delivered'")
    .count())
time_before = time.time() - start

# Consulta na versão ATUAL (com Z-ORDER)
start = time.time()
result_after = (spark.table("bronze.orders")
    .where("order_status = 'delivered'")
    .count())
time_after = time.time() - start

print(f"Consulta: WHERE order_status = 'delivered'")
print(f"  Versão anterior: {result_before:,} registros em {time_before:.2f}s")
print(f"  Versão otimizada: {result_after:,} registros em {time_after:.2f}s")

# COMMAND ----------

# Consulta filtrada por customer_id
start = time.time()
sample_customer = spark.table("bronze.orders").select("customer_id").first().customer_id
result = spark.table("bronze.orders").where(F.col("customer_id") == sample_customer).count()
time_zorder = time.time() - start
print(f"Consulta por customer_id com Z-ORDER: {result} registro(s) em {time_zorder:.3f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Histórico — Operações registradas

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY pb_brasilmart.bronze.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. VACUUM — Limpeza de Arquivos Obsoletos
# MAGIC
# MAGIC Após o OPTIMIZE, os arquivos antigos (small files) ficam no storage
# MAGIC para suportar Time Travel. O `VACUUM` remove arquivos que não são mais
# MAGIC referenciados por nenhuma versão dentro do período de retenção (padrão 7 dias).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dry run: mostra o que seria removido (sem deletar)
# MAGIC VACUUM pb_brasilmart.bronze.orders DRY RUN;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo
# MAGIC
# MAGIC | Operação | O que faz | Quando usar |
# MAGIC |----------|-----------|-------------|
# MAGIC | **OPTIMIZE** | Compacta small files em arquivos maiores | Após muitas escritas incrementais |
# MAGIC | **Z-ORDER** | Reorganiza dados pela coluna escolhida | Colunas usadas em filtros `WHERE` |
# MAGIC | **VACUUM** | Remove arquivos obsoletos do storage | Manutenção periódica (economia de custo) |
# MAGIC
# MAGIC O Z-ORDER em `customer_id` e `order_status` acelera as consultas mais
# MAGIC frequentes do projeto: visão 360° do cliente e análise por status de pedido.
