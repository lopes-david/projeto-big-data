# Databricks notebook source
# MAGIC %md
# MAGIC # TP3 — 4.1 Monitoramento de Recursos AWS e Databricks
# MAGIC
# MAGIC **Objetivo:** Apresentar evidências de uso de todos os recursos do projeto
# MAGIC BrasilMart no TP3, cobrindo AWS (S3, Redshift, Glue, Step Functions) e
# MAGIC Databricks (DLT pipelines, Workflows, Unity Catalog, SQL Warehouse).

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

print(f"Relatório gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Databricks — Pipeline DLT Silver (TP3 1.1–1.4)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG pb_brasilmart;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Tabelas Silver — Contagem e Tamanho

# COMMAND ----------

silver_tables = [
    "orders", "customers", "items", "payments", "reviews",
    "products", "sellers", "geolocation", "category_translation",
    "orders_enriched", "items_enriched"
]

print(f"{'Tabela Silver':<30} {'Registros':>12} {'Arquivos':>10} {'Tamanho (MB)':>15}")
print("-" * 72)

total_records = 0
total_files = 0
total_size_mb = 0

for t in silver_tables:
    try:
        count = spark.table(f"silver.{t}").count()
        detail = spark.sql(f"DESCRIBE DETAIL pb_brasilmart.silver.{t}").first()
        n_files = detail.numFiles
        size_mb = detail.sizeInBytes / 1024 / 1024
        total_records += count
        total_files += n_files
        total_size_mb += size_mb
        print(f"{t:<30} {count:>12,} {n_files:>10} {size_mb:>15.2f}")
    except Exception as e:
        print(f"{t:<30} {'ERRO':>12} {'—':>10} {'—':>15}  {str(e)[:40]}")

print("-" * 72)
print(f"{'TOTAL SILVER':<30} {total_records:>12,} {total_files:>10} {total_size_mb:>15.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Tabelas Silver CDC — APPLY CHANGES INTO

# COMMAND ----------

cdc_tables = [
    "silver_customers_atualizada",
    "silver_sellers_atualizada",
    "silver_products_atualizada",
    "silver_category_translation_atualizada"
]

print(f"{'Tabela CDC':<45} {'Registros':>12}")
print("-" * 60)

for t in cdc_tables:
    try:
        count = spark.table(f"silver.{t}").count()
        print(f"{t:<45} {count:>12,}")
    except Exception as e:
        print(f"{t:<45} {'N/A':>12}  (pipeline CDC não executado ainda)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Expectations — Métricas de Qualidade DLT
# MAGIC
# MAGIC As métricas abaixo são visíveis no **DLT Pipeline UI → Event Log**.
# MAGIC Aqui documentamos as regras implementadas e seu comportamento esperado.

# COMMAND ----------

expectations_report = [
    {"regra": "Valor_Pagamento_Valido",  "acao": "DROP ROW",    "tabela": "payments",        "condicao": "payment_value BETWEEN 0.01 AND 99999.99"},
    {"regra": "Order_ID_Obrigatorio",    "acao": "FAIL UPDATE",  "tabela": "orders_enriched", "condicao": "order_id IS NOT NULL"},
    {"regra": "Customer_ID_Obrigatorio", "acao": "FAIL UPDATE",  "tabela": "orders_enriched", "condicao": "customer_id IS NOT NULL"},
    {"regra": "Entrega_Atraso_Alerta",   "acao": "WARN",         "tabela": "orders",          "condicao": "delta_entrega_dias <= 30 OR IS NULL"},
    {"regra": "Score_Range_Valido",      "acao": "WARN",         "tabela": "reviews",         "condicao": "review_score BETWEEN 1 AND 5"},
    {"regra": "Preco_Alerta_Alto",       "acao": "WARN",         "tabela": "items",           "condicao": "price < 10000"},
]

print(f"{'Regra':<30} {'Ação':<15} {'Tabela':<20} {'Condição'}")
print("-" * 100)
for e in expectations_report:
    print(f"{e['regra']:<30} {e['acao']:<15} {e['tabela']:<20} {e['condicao']}")

print(f"\nTotal de expectations: {len(expectations_report)}")
print("  DROP ROW:    1 regra (descarta linhas inválidas)")
print("  FAIL UPDATE: 2 regras (interrompe pipeline se violadas)")
print("  WARN:        3 regras (registra alerta, mantém dados)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Verificação de Qualidade — Contagem de Violações WARN

# COMMAND ----------

atraso_extremo = spark.table("silver.orders").where("delta_entrega_dias > 30").count()
score_invalido = spark.table("silver.reviews").where("review_score NOT BETWEEN 1 AND 5").count()
preco_alto = spark.table("silver.items").where("price >= 10000").count()

print("Registros que disparariam WARN (mantidos nos dados):")
print(f"  Entrega_Atraso_Alerta (>30 dias):  {atraso_extremo:,}")
print(f"  Score_Range_Valido (fora 1-5):      {score_invalido:,}")
print(f"  Preco_Alerta_Alto (>= R$10.000):    {preco_alto:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Databricks — Bronze (referência)

# COMMAND ----------

bronze_tables = ["orders", "customers", "items", "payments", "reviews", "products", "sellers", "geolocation", "category_translation"]

print(f"{'Tabela Bronze':<25} {'Registros':>12} {'Arquivos':>10} {'Tamanho (MB)':>15} {'Tipo':<10}")
print("-" * 78)

total_b_size = 0
for t in bronze_tables:
    try:
        count = spark.table(f"bronze.{t}").count()
        detail = spark.sql(f"DESCRIBE DETAIL pb_brasilmart.bronze.{t}").first()
        size_mb = detail.sizeInBytes / 1024 / 1024
        total_b_size += size_mb
        print(f"{t:<25} {count:>12,} {detail.numFiles:>10} {size_mb:>15.2f} {'MANAGED':<10}")
    except Exception as e:
        print(f"{t:<25} {'ERRO':>12}")

print(f"\nTotal Bronze: {total_b_size:.2f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Histórico de Operações Delta — Auditoria

# COMMAND ----------

for t in ["orders", "sellers", "products"]:
    print(f"\n--- HISTORY: bronze.{t} ---")
    history = spark.sql(f"DESCRIBE HISTORY pb_brasilmart.bronze.{t} LIMIT 5")
    display(history.select("version", "timestamp", "operation", "operationParameters"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Databricks — Compute e Warehouse

# COMMAND ----------

print("=== SQL Warehouse ===")
print("  Nome:       Serverless Starter Warehouse")
print("  Tipo:       PRO (Serverless)")
print("  Tamanho:    2X-Small")
print("  Auto-stop:  10 minutos")
print("  Photon:     Habilitado")
print("  Modelo:     Pay-per-use (sem cluster ocioso)")
print()
print("=== Unity Catalog ===")
print("  Catálogo:   pb_brasilmart")
print("  Schemas:    bronze (9 tabelas), silver (11+ tabelas), gold (4 tabelas)")
print("  Volumes:    raw_files, staging_files, export_files")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. AWS — S3 Storage por Camada
# MAGIC
# MAGIC | Camada | Bucket | Uso |
# MAGIC |--------|--------|-----|
# MAGIC | **Raw** | pb-raw-brasilmart-234828142988 | ~137 MB (9 CSVs originais) |
# MAGIC | **Bronze** | pb-bronze-brasilmart-234828142988 | ~20 MB (Delta + _delta_log) |
# MAGIC | **Silver** | pb-silver-brasilmart-234828142988 | Staging temporário (Redshift COPY) |
# MAGIC | **Gold** | pb-gold-brasilmart-234828142988 | Exports e relatórios |
# MAGIC
# MAGIC **Políticas aplicadas:** Versioning, SSE-S3, Lifecycle (Standard-IA → Glacier), Block Public Access.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. AWS — Redshift Serverless
# MAGIC
# MAGIC | Parâmetro | Valor |
# MAGIC |-----------|-------|
# MAGIC | Workgroup | default-workgroup |
# MAGIC | Capacidade | 128 RPUs (serverless, auto-scale) |
# MAGIC | Endpoint | default-workgroup.234828142988.sa-east-1.redshift-serverless.amazonaws.com:5439 |
# MAGIC | Database | dev |
# MAGIC | Schemas | pb_bronze, pb_silver, pb_gold, **raw_databricks** (novo TP3) |
# MAGIC | IAM Roles | redshift-s3-copy-role, RedshiftSpectrumRole |
# MAGIC | Cobrança | Pay-per-query (sem cluster ocioso) |
# MAGIC
# MAGIC ### Schema `raw_databricks` (TP3 3.1)
# MAGIC | Tabela | DistKey | SortKey |
# MAGIC |--------|---------|---------|
# MAGIC | orders | order_id | order_purchase_timestamp |
# MAGIC | customers | customer_id | customer_state |
# MAGIC | items | order_id | product_id |
# MAGIC | payments | order_id | payment_type |
# MAGIC | reviews | order_id | review_score |
# MAGIC | products | product_id | product_category |
# MAGIC | sellers | seller_id | seller_state |
# MAGIC | orders_enriched | order_id | order_purchase_timestamp |
# MAGIC | items_enriched | order_id | product_category |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. AWS — Glue Data Catalog e Lake Formation
# MAGIC
# MAGIC | Recurso | Configuração |
# MAGIC |---------|-------------|
# MAGIC | **Databases Glue** | pb_raw_brasilmart, pb_bronze_brasilmart, pb_silver_brasilmart, pb_gold_brasilmart |
# MAGIC | **Glue Job** | pb-batch-ingestion-orders-brasilmart (Glue 4.0, 2× G.1X) |
# MAGIC | **Lake Formation Admin** | root (Full Access) |
# MAGIC | **Buckets registrados** | 4 (raw, bronze, silver, gold) |
# MAGIC | **GlueETLRole** | DATA_LOCATION_ACCESS em raw + bronze |
# MAGIC | **Criptografia** | SSE-S3 em todos os buckets |
# MAGIC | **Lifecycle** | Raw=7 anos, Bronze=3 anos, Silver=2 anos, Gold=1 ano |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Orquestração — Evidências (TP3 2.1–2.3)
# MAGIC
# MAGIC ### 7.1 AWS Step Functions
# MAGIC | State | Tipo | Descrição |
# MAGIC |-------|------|-----------|
# MAGIC | Carga_Inicial_Redshift | Task | COPY S3 → Redshift via Redshift Data API |
# MAGIC | Disparo_Databricks_DLT | Task | HTTP invoke → Databricks Jobs API |
# MAGIC | Finalizacao_dbt | Task | Lambda → dbt run (staging + marts) |
# MAGIC | Falha_* | Fail | 3 estados de erro (Redshift, Databricks, dbt) |
# MAGIC
# MAGIC ### 7.2 Databricks Workflow
# MAGIC | Task | Tipo | Dependência |
# MAGIC |------|------|-------------|
# MAGIC | Executar_DLT | pipeline_task | — (início) |
# MAGIC | Executar_Notebook_Validacao | notebook_task | DLT → success |
# MAGIC | Otimizar_Tabela_Gold | notebook_task | Validação → success |
# MAGIC | Enviar_Alerta_Falha | notebook_task | DLT → **failed** (ramificação condicional) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Projeto dbt — Integração Híbrida (TP3 3.2)
# MAGIC
# MAGIC | Aspecto | TP2 (antes) | TP3 (agora) |
# MAGIC |---------|-------------|-------------|
# MAGIC | **Source** | `bronze` (pb_bronze) | `databricks_silver` (raw_databricks) |
# MAGIC | **Staging lê de** | Dados brutos + limpeza SQL | Silver do Databricks (já limpo pelo DLT) |
# MAGIC | **Staging faz** | Limpeza + transformação | Pass-through (dados já transformados) |
# MAGIC | **Marts** | Sem mudança | Sem mudança (usam `ref('stg_*')`) |
# MAGIC | **Novos campos** | — | tempo_*_seg, product_volume_cm3, porte_produto, payment_group, review_sentiment |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Resumo de Custos e Otimizações
# MAGIC
# MAGIC | Serviço | Uso TP3 | Custo Estimado | Otimização |
# MAGIC |---------|---------|----------------|------------|
# MAGIC | **S3** | ~160 MB (4 buckets) | ~$0.01/mês | Lifecycle → Glacier |
# MAGIC | **Glue** | 1 job batch (3m25s) | ~$0.15 | Mínimo de workers (2× G.1X) |
# MAGIC | **Redshift** | 128 RPU serverless | Pay-per-query | Sem cluster ocioso |
# MAGIC | **Databricks** | Serverless Starter | Pay-per-use | Auto-stop 10min |
# MAGIC | **DLT Pipeline** | 11 tabelas Silver | Incluso no compute | Batch (não continuous) |
# MAGIC | **Step Functions** | 1 state machine | ~$0.025/execução | Standard (não Express) |
# MAGIC | **Lake Formation** | Governança | Sem custo adicional | — |
# MAGIC
# MAGIC **Estratégias aplicadas:**
# MAGIC - Tudo serverless (Redshift, Databricks, DLT) — zero custo ocioso
# MAGIC - DLT em modo Triggered (batch) em vez de Continuous — executa só quando necessário
# MAGIC - DistKey/SortKey no Redshift — queries mais rápidas, menos RPUs consumidos
# MAGIC - OPTIMIZE + Z-ORDER no Delta Lake — menos arquivos, menos I/O
# MAGIC - Lifecycle no S3 — redução de custo a longo prazo

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 10. Inventário Completo do Projeto (TP1 → TP3)

# COMMAND ----------

print("=== INVENTÁRIO: Notebooks Databricks ===")
notebooks = {
    "TP1": [
        "PB-01_ingestao_json_aninhado",
        "PB-02_streaming_simulado",
        "PB-03_limpeza_bronze",
    ],
    "TP2": [
        "PB-TP2-01_delta_lake_conversao",
        "PB-TP2-02_unity_catalog_setup",
        "PB-TP2-03_bronze_unity_catalog",
        "PB-TP2-04_merge_time_travel",
        "PB-TP2-05_optimize_zorder",
        "PB-TP2-06_monitoramento",
    ],
    "TP3": [
        "PB-TP3-01_dlt_silver (DLT Pipeline — 11 tabelas)",
        "PB-TP3-02_dlt_cdc_silver (APPLY CHANGES — 4 tabelas CDC)",
        "PB-TP3-03_validacao_silver (Notebook de validação)",
        "PB-TP3-04_alerta_falha (Alerta simulado — branch condicional)",
        "PB-TP3-05_otimizar_gold (OPTIMIZE + Z-ORDER + VACUUM)",
        "PB-TP3-06_export_silver_redshift (Databricks → Redshift)",
        "PB-TP3-07_monitoramento (este notebook)",
    ],
}

total = 0
for tp, nbs in notebooks.items():
    print(f"\n{tp}:")
    for nb in nbs:
        print(f"  - {nb}")
    total += len(nbs)

print(f"\nTotal: {total} notebooks")

# COMMAND ----------

print("\n=== INVENTÁRIO: Infra e Orquestração ===")
print("  AWS Step Functions:    infra/aws/step_functions_workflow.json")
print("  Databricks Workflow:   infra/databricks/workflow_silver_gold.json")
print("  Glue Job:              pb-batch-ingestion-orders-brasilmart")
print("  dbt Project:           dbt/pb_brasilmart/ (7 staging + 4 marts)")

# COMMAND ----------

print("\n=== INVENTÁRIO: Unity Catalog ===")
schemas = spark.sql("SHOW SCHEMAS IN pb_brasilmart").collect()
for schema in schemas:
    schema_name = schema.databaseName
    tables = spark.sql(f"SHOW TABLES IN pb_brasilmart.{schema_name}").collect()
    print(f"\npb_brasilmart.{schema_name}: {len(tables)} tabelas")
    for t in tables:
        print(f"  - {t.tableName}")
