# Databricks notebook source
# MAGIC %md
# MAGIC # TP5 — Atividade 5.1: Monitoramento de Recursos AWS e Databricks
# MAGIC
# MAGIC **Objetivo:** Evidenciar o uso de todos os recursos configurados no TP5
# MAGIC e apresentar o inventário completo da plataforma (TP1 → TP5).

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

print(f"Relatório TP5 gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Catálogo: pb_brasilmart")
print(f"Workspace: dbc-5f5c64be-0b42.cloud.databricks.com")
print(f"Conta AWS: 234828142988 (sa-east-1)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Recursos Novos no TP5

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Feedback Loop — Redshift ↔ Databricks ML
# MAGIC
# MAGIC | Recurso | Tipo | Detalhes |
# MAGIC |---------|------|---------|
# MAGIC | `pb_gold.features_ml` | Tabela Redshift (dbt) | 13 features pré-agregadas para ML, materialized=table |
# MAGIC | `pb_gold.predicoes_databricks_ml` | Tabela Redshift | Predições do modelo ML devolvidas ao DW |
# MAGIC | Spark-Redshift connector | Conector | Leitura features_ml + escrita predicoes_databricks_ml |
# MAGIC | MLflow Model (Production) | Model Registry | `pb-brasilmart-predicao-atraso` → stage Production |
# MAGIC | S3 staging | Bucket existente | `pb-silver-brasilmart-234828142988/redshift-staging` |
# MAGIC
# MAGIC ### 1.2 GenAI — Análise de Sentimento
# MAGIC
# MAGIC | Recurso | Tipo | Detalhes |
# MAGIC |---------|------|---------|
# MAGIC | `ai_analyze_sentiment` | AI Function (DBSQL) | Foundation Model API, 1000 reviews analisadas |
# MAGIC | SQL Warehouse | Serverless SQL | Execução das queries GenAI |
# MAGIC
# MAGIC ### 1.3 RAG Conceitual
# MAGIC
# MAGIC | Recurso | Tipo | Detalhes |
# MAGIC |---------|------|---------|
# MAGIC | Pipeline RAG | Conceitual | Documentado: chunking → embedding → Vector Search → LLM |
# MAGIC | Componentes planejados | Databricks | Vector Search, Foundation Model API, Agent Framework |
# MAGIC
# MAGIC ### 1.4 Observabilidade
# MAGIC
# MAGIC | Recurso | Tipo | Detalhes |
# MAGIC |---------|------|---------|
# MAGIC | Lakehouse Monitoring | Monitor Snapshot | `dim_clientes_rfm` + `predicoes_databricks_ml` |
# MAGIC | Profile Metrics Tables | Auto-geradas | `*_profile_metrics` (2 tabelas) |
# MAGIC | Drift Metrics Tables | Auto-geradas | `*_drift_metrics` (2 tabelas) |
# MAGIC | System Tables | Billing/Compute | `system.billing.usage`, `system.billing.list_prices` |
# MAGIC
# MAGIC ### 1.5 Dashboard BI
# MAGIC
# MAGIC | Recurso | Tipo | Detalhes |
# MAGIC |---------|------|---------|
# MAGIC | QuickSight Data Source | Redshift connection | `pb-brasilmart-redshift-ds` |
# MAGIC | QuickSight DataSets | 5 datasets | vendas, clientes, sellers, produtos, predições |
# MAGIC | QuickSight Dashboard | 5 abas | KPIs, RFM, Vendedores, Produtos, Inteligência ML |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. AWS — Recursos Provisionados (estado completo)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Amazon S3
# MAGIC
# MAGIC | Bucket | Conteúdo | Config |
# MAGIC |--------|---------|--------|
# MAGIC | `pb-raw-brasilmart-234828142988` | 9 CSVs originais + JSON aninhado (137 MB) | Versioning, SSE-S3, lifecycle 365d→IA |
# MAGIC | `pb-bronze-brasilmart-234828142988` | Parquet particionado (19.6 MB, 2.462 obj) | Versioning, SSE-S3, lifecycle 180d→IA |
# MAGIC | `pb-silver-brasilmart-234828142988` | Staging Redshift COPY (temporário) | Versioning, SSE-S3, lifecycle 90d→IA |
# MAGIC | `pb-gold-brasilmart-234828142988` | Exports e relatórios | Versioning, SSE-S3, lifecycle 90d→IA |
# MAGIC | `pb-brasilmart-codepipeline-artifacts-*` | Artefatos CI/CD (TP4) | Lifecycle automático |
# MAGIC | `brasilmart-terraform-state` | Terraform state (backend) | Versioning |
# MAGIC
# MAGIC ### 2.2 AWS Glue
# MAGIC
# MAGIC | Recurso | Detalhes |
# MAGIC |---------|---------|
# MAGIC | Databases | 4: `pb_raw_brasilmart`, `pb_bronze_brasilmart`, `pb_silver_brasilmart`, `pb_gold_brasilmart` |
# MAGIC | Job | `pb-batch-ingestion-orders-brasilmart` (Glue 4.0, 2x G.1X) |
# MAGIC | PII Tags | 13 colunas taggeadas em 5 tabelas (TP4) |
# MAGIC
# MAGIC ### 2.3 Lake Formation
# MAGIC
# MAGIC | Recurso | Detalhes |
# MAGIC |---------|---------|
# MAGIC | Admin | root (Full Access) |
# MAGIC | Data Lake Locations | 4 buckets registrados |
# MAGIC | Column-Level Security | 4 tabelas com ExcludedColumnNames (Analista Jr.) |
# MAGIC | GlueETLRole | DATA_LOCATION_ACCESS em raw + bronze |
# MAGIC
# MAGIC ### 2.4 Amazon Redshift Serverless
# MAGIC
# MAGIC | Recurso | Detalhes |
# MAGIC |---------|---------|
# MAGIC | Workgroup | `default-workgroup` (128 RPU) |
# MAGIC | Database | `dev` |
# MAGIC | Schemas | `pb_bronze`, `pb_silver`, `pb_gold`, `raw_databricks` |
# MAGIC | IAM Roles | `redshift-s3-copy-role`, `RedshiftSpectrumRole` |
# MAGIC
# MAGIC ### 2.5 Orquestração e CI/CD
# MAGIC
# MAGIC | Recurso | Detalhes |
# MAGIC |---------|---------|
# MAGIC | Step Functions | `pb-brasilmart-orchestration` (7 states) |
# MAGIC | CodePipeline | `pb-brasilmart-cicd` (Source → Infra → dbt) |
# MAGIC | CodeBuild | `pb-brasilmart-dbt-build`, `pb-brasilmart-infra-build` |
# MAGIC | SNS Topic | `pb-brasilmart-alertas` → e-mail |
# MAGIC | CloudWatch Alarms | 2 (ExecutionsFailed, ExecutionTime) |
# MAGIC
# MAGIC ### 2.6 Amazon QuickSight (TP5)
# MAGIC
# MAGIC | Recurso | Detalhes |
# MAGIC |---------|---------|
# MAGIC | Data Source | `pb-brasilmart-redshift-ds` (Redshift Serverless) |
# MAGIC | DataSets | 5 (vendas, clientes, sellers, produtos, predições ML) |
# MAGIC | Dashboard | 5 abas (KPIs, RFM, Vendedores, Produtos, Inteligência ML) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Databricks — Unity Catalog (inventário completo)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG pb_brasilmart;

# COMMAND ----------

print("=== Unity Catalog — Inventário Completo ===\n")
total_tables = 0
total_rows = 0

for schema_name in ["bronze", "silver", "gold"]:
    try:
        tables = spark.sql(f"SHOW TABLES IN pb_brasilmart.{schema_name}").collect()
        total_tables += len(tables)
        schema_rows = 0
        print(f"pb_brasilmart.{schema_name}: {len(tables)} tabelas")
        for t in tables:
            full_name = f"pb_brasilmart.{schema_name}.{t.tableName}"
            try:
                count = spark.table(full_name).count()
                detail = spark.sql(f"DESCRIBE DETAIL {full_name}").first()
                size_mb = (detail.sizeInBytes or 0) / 1024 / 1024
                schema_rows += count
                print(f"  {t.tableName:<40} {count:>10,} rows  {size_mb:>8.2f} MB")
            except Exception:
                print(f"  {t.tableName:<40} (detalhes indisponíveis)")
        total_rows += schema_rows
        print(f"  {'SUBTOTAL':<40} {schema_rows:>10,} rows")
        print()
    except Exception as e:
        print(f"pb_brasilmart.{schema_name}: ERRO — {str(e)[:50]}")

print(f"Total geral: {total_tables} tabelas, {total_rows:,} registros")

# COMMAND ----------

# Volumes
print("=== Unity Catalog — Volumes ===")
try:
    volumes = spark.sql("SHOW VOLUMES IN pb_brasilmart.bronze").collect()
    for v in volumes:
        print(f"  bronze.{v.volume_name}")
except Exception:
    pass
try:
    volumes = spark.sql("SHOW VOLUMES IN pb_brasilmart.silver").collect()
    for v in volumes:
        print(f"  silver.{v.volume_name}")
except Exception:
    pass
try:
    volumes = spark.sql("SHOW VOLUMES IN pb_brasilmart.gold").collect()
    for v in volumes:
        print(f"  gold.{v.volume_name}")
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Databricks — DLT Pipelines

# COMMAND ----------

print("=== DLT Pipelines ===\n")
dlt_pipelines = {
    "pb-brasilmart-silver": {
        "tabelas": 11,
        "tipo": "Batch (Bronze → Silver)",
        "expectations": "6 (DROP ROW, FAIL UPDATE, WARN)",
        "enriquecidas": "orders_enriched, items_enriched",
    },
    "pb-brasilmart-silver-cdc": {
        "tabelas": 4,
        "tipo": "Streaming (APPLY CHANGES INTO)",
        "expectations": "N/A",
        "enriquecidas": "customers, sellers, products, category_translation",
    },
}

for name, config in dlt_pipelines.items():
    print(f"  Pipeline: {name}")
    for k, v in config.items():
        print(f"    {k}: {v}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Databricks — MLflow (Model Registry + Serving)

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

client = MlflowClient()
model_name = "pb-brasilmart-predicao-atraso"
experiment_name = "/Users/david.lopes@al.infnet.edu.br/pb-brasilmart-predicao-atraso"

# Experiment
try:
    experiment = client.get_experiment_by_name(experiment_name)
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["start_time DESC"]
    )
    print("=== MLflow Experiment ===")
    print(f"  Nome:          {experiment.name}")
    print(f"  Experiment ID: {experiment.experiment_id}")
    print(f"  Total runs:    {len(runs)}")
    print(f"\n  {'Run Name':<42} {'Flavor':<10} {'AUC-ROC':>10} {'F1':>10}")
    print("  " + "-" * 78)
    for r in runs:
        name = r.info.run_name or r.info.run_id[:8]
        flavor = r.data.tags.get("flavor", "spark")
        auc = r.data.metrics.get("auc_roc", 0)
        f1_val = r.data.metrics.get("f1_score", r.data.metrics.get("f1_weighted", 0))
        print(f"  {name:<42} {flavor:<10} {auc:>10.4f} {f1_val:>10.4f}")
except Exception as e:
    print(f"  Experiment: {str(e)[:60]}")

# COMMAND ----------

# Model Registry
try:
    model = client.get_registered_model(model_name)
    versions = client.search_model_versions(f"name='{model_name}'")
    print(f"\n=== MLflow Model Registry ===")
    print(f"  Modelo:  {model.name}")
    print(f"  Tags:    {model.tags}")
    print(f"  Versões: {len(versions)}")
    print(f"\n  {'Versão':<10} {'Stage':<15} {'Run ID':<35} {'Status'}")
    print("  " + "-" * 75)
    for v in sorted(versions, key=lambda x: int(x.version)):
        print(f"  v{v.version:<9} {v.current_stage:<15} {v.run_id:<35} {v.status}")
except Exception as e:
    print(f"  Model Registry: {str(e)[:60]}")

# COMMAND ----------

# Model Serving
import requests, json

try:
    databricks_host = spark.conf.get("spark.databricks.workspaceUrl")
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {"Authorization": f"Bearer {token}"}
    endpoint_name = "pb-brasilmart-atraso-endpoint"

    resp = requests.get(
        f"https://{databricks_host}/api/2.0/serving-endpoints/{endpoint_name}",
        headers=headers,
    )
    if resp.status_code == 200:
        ep = resp.json()
        state = ep.get("state", {})
        served = ep.get("config", {}).get("served_entities", [{}])
        print(f"\n=== Model Serving Endpoint ===")
        print(f"  Nome:    {ep.get('name')}")
        print(f"  Estado:  {state.get('ready', 'UNKNOWN')}")
        if served:
            e = served[0]
            print(f"  Modelo:  {e.get('entity_name', 'N/A')} v{e.get('entity_version', '?')}")
            print(f"  Size:    {e.get('workload_size', 'N/A')}")
            print(f"  Scale-0: {e.get('scale_to_zero_enabled', 'N/A')}")
        print(f"  URL:     https://{databricks_host}/serving-endpoints/{endpoint_name}/invocations")
    else:
        print(f"\n  Endpoint '{endpoint_name}': {resp.status_code}")
except Exception as e:
    print(f"\n  Serving: {str(e)[:60]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Databricks — Lakehouse Monitoring (TP5)

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

monitors = [
    "pb_brasilmart.gold.dim_clientes_rfm",
    "pb_brasilmart.gold.predicoes_databricks_ml",
]

print("=== Lakehouse Monitoring ===\n")
for table in monitors:
    short = table.split(".")[-1]
    try:
        mon = w.quality_monitors.get(table_name=table)
        print(f"  {short}:")
        print(f"    Status:        {mon.status}")
        print(f"    Profile Table: {mon.profile_metrics_table_name}")
        print(f"    Drift Table:   {mon.drift_metrics_table_name}")
        print(f"    Dashboard:     {mon.dashboard_id}")
        print()
    except Exception as e:
        print(f"  {short}: não configurado ({str(e)[:40]})")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Redshift — Tabelas no Schema pb_gold

# COMMAND ----------

REDSHIFT_HOST = "default-workgroup.234828142988.sa-east-1.redshift-serverless.amazonaws.com"
REDSHIFT_PORT = "5439"
REDSHIFT_DB = "dev"
REDSHIFT_USER = dbutils.secrets.get(scope="pb-brasilmart", key="redshift-user")
REDSHIFT_PASS = dbutils.secrets.get(scope="pb-brasilmart", key="redshift-password")
REDSHIFT_URL = f"jdbc:redshift://{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}"
S3_STAGING = "s3://pb-silver-brasilmart-234828142988/redshift-staging"
IAM_ROLE = "arn:aws:iam::234828142988:role/redshift-s3-copy-role"

print("=== Redshift — Schemas e Tabelas ===\n")

for schema in ["raw_databricks", "pb_gold"]:
    try:
        df_tables = (
            spark.read
            .format("io.databricks.spark.redshift")
            .option("url", REDSHIFT_URL)
            .option("user", REDSHIFT_USER)
            .option("password", REDSHIFT_PASS)
            .option("query", f"""
                SELECT tablename,
                       (SELECT COUNT(*) FROM {schema}.\"\" || tablename) AS row_count
                FROM pg_tables
                WHERE schemaname = '{schema}'
                ORDER BY tablename
            """)
            .option("tempdir", S3_STAGING)
            .option("aws_iam_role", IAM_ROLE)
            .load()
        )
        print(f"  Schema: {schema}")
        df_tables.show(20, truncate=False)
    except Exception:
        # Fallback: list tables individually
        try:
            df_list = (
                spark.read
                .format("io.databricks.spark.redshift")
                .option("url", REDSHIFT_URL)
                .option("user", REDSHIFT_USER)
                .option("password", REDSHIFT_PASS)
                .option("query", f"SELECT tablename FROM pg_tables WHERE schemaname = '{schema}' ORDER BY tablename")
                .option("tempdir", S3_STAGING)
                .option("aws_iam_role", IAM_ROLE)
                .load()
            )
            tables = [row.tablename for row in df_list.collect()]
            print(f"  Schema: {schema} ({len(tables)} tabelas)")
            for t in tables:
                try:
                    cnt_df = (
                        spark.read
                        .format("io.databricks.spark.redshift")
                        .option("url", REDSHIFT_URL)
                        .option("user", REDSHIFT_USER)
                        .option("password", REDSHIFT_PASS)
                        .option("query", f"SELECT COUNT(*) AS cnt FROM {schema}.{t}")
                        .option("tempdir", S3_STAGING)
                        .option("aws_iam_role", IAM_ROLE)
                        .load()
                    )
                    cnt = cnt_df.first()["cnt"]
                    print(f"    {t:<40} {cnt:>10,} rows")
                except Exception:
                    print(f"    {t:<40} (contagem indisponível)")
            print()
        except Exception as e:
            print(f"  Schema {schema}: ERRO — {str(e)[:60]}")
            print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. dbt — Projeto pb_brasilmart

# COMMAND ----------

print("=== dbt — Modelos ===\n")

dbt_models = {
    "Staging (7 views — pb_silver)": [
        "stg_orders — Orders limpos com delta_entrega e status_entrega",
        "stg_customers — Clientes com estado/cidade padronizados",
        "stg_items — Itens com total_item_value calculado",
        "stg_payments — Pagamentos com payment_group",
        "stg_reviews — Avaliações com review_sentiment e tempo_resposta",
        "stg_products — Produtos com categoria EN, peso kg, volume cm³",
        "stg_sellers — Vendedores com CEP/cidade padronizados",
    ],
    "Marts (5 tables — pb_gold)": [
        "dim_clientes_rfm — Segmentação RFM (TABLE, dist=customer_unique_id)",
        "dim_sellers_score — Score composto de vendedores (TABLE, dist=seller_id)",
        "dim_produtos_performance — Performance de produtos (TABLE, dist=product_id)",
        "fato_vendas_diarias — GMV diário (INCREMENTAL, dist=data_venda)",
        "features_ml — Features pré-agregadas para ML (TABLE, dist=order_id) [TP5]",
    ],
}

for category, models in dbt_models.items():
    print(f"  {category}:")
    for m in models:
        print(f"    - {m}")
    print()

print("  Sources:")
print("    - databricks_silver (raw_databricks) — 9 tabelas Silver do Databricks")
print("    - bronze (pb_bronze) — legado")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Inventário Completo: Notebooks (TP1 → TP5)

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
        "PB-TP3-02_dlt_cdc_silver (APPLY CHANGES INTO — 4 tabelas)",
        "PB-TP3-03_validacao_silver",
        "PB-TP3-04_alerta_falha",
        "PB-TP3-05_otimizar_gold",
        "PB-TP3-06_export_silver_redshift",
        "PB-TP3-07_monitoramento",
    ],
    "TP4": [
        "PB-TP4-01_cicd_devops (CI/CD + DABs + CloudWatch)",
        "PB-TP4-02_descoberta_pii (Scan PII + Glue tags)",
        "PB-TP4-03_permissoes_finas (Column-Level Security)",
        "PB-TP4-04_rls_lineage (Row-Level Security + Lineage)",
        "PB-TP4-05_mlops_mlflow (MLflow Tracking — 3+1 runs)",
        "PB-TP4-06_model_registry_serving (sklearn + REST endpoint)",
        "PB-TP4-07_monitoramento",
    ],
    "TP5": [
        "PB-TP5-01_feedback_loop_ml (Redshift→Databricks→ML→Redshift)",
        "PB-TP5-02_genai_sentiment (ai_analyze_sentiment — reviews)",
        "PB-TP5-02b_rag_conceitual (RAG pipeline — conceitual)",
        "PB-TP5-03_lakehouse_monitoring (Monitor + Dashboard qualidade)",
        "PB-TP5-03b_system_tables_dbu (System Tables — consumo DBUs)",
        "PB-TP5-03c_tendencias_relatorio (Data Fabric + Lambda vs Kappa)",
        "PB-TP5-04_dashboard_quicksight (QuickSight — queries + layout)",
        "PB-TP5-05_monitoramento (este notebook)",
    ],
}

total = 0
for tp, nbs in notebooks.items():
    print(f"\n{tp}: ({len(nbs)} notebooks)")
    for nb in nbs:
        print(f"  - {nb}")
    total += len(nbs)
print(f"\nTotal: {total} notebooks")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 10. Inventário: Infraestrutura como Código

# COMMAND ----------

print("=== Infraestrutura como Código ===")

iac_files = {
    "Terraform": [
        "infra/terraform/main.tf — Provider + variáveis",
        "infra/terraform/s3.tf — 4 buckets S3",
        "infra/terraform/glue.tf — Glue databases + job + crawler",
        "infra/terraform/lake_formation.tf — Lake Formation + roles",
        "infra/terraform/codepipeline.tf — CodePipeline + CodeBuild (TP4)",
        "infra/terraform/monitoring.tf — SNS + CloudWatch alarms (TP4)",
        "infra/terraform/column_security.tf — Column-Level Security (TP4)",
    ],
    "AWS CLI Scripts": [
        "infra/aws/setup_s3.sh — Setup buckets",
        "infra/aws/setup_lake_formation.sh — Lake Formation + Glue",
        "infra/aws/setup_monitoring.sh — CloudWatch + SNS (TP4)",
        "infra/aws/setup_pii_tagging.sh — PII tags Glue (TP4)",
        "infra/aws/setup_column_security.sh — Column-Level Security (TP4)",
        "infra/aws/setup_quicksight.sh — QuickSight dashboard (TP5)",
    ],
    "Orquestração": [
        "infra/aws/step_functions_workflow.json — Step Functions",
        "infra/aws/codepipeline/ — CodePipeline + buildspecs (TP4)",
        "infra/databricks/workflow_silver_gold.json — Databricks Workflow",
        "infra/databricks/bundle.yml — Databricks Asset Bundle (TP4)",
    ],
    "dbt": [
        "dbt/pb_brasilmart/profiles.yml — Redshift (dev + prod)",
        "dbt/pb_brasilmart/dbt_project.yml — Config do projeto",
        "dbt/pb_brasilmart/models/staging/ — 7 views (Silver)",
        "dbt/pb_brasilmart/models/marts/ — 5 tabelas (Gold) [+features_ml TP5]",
    ],
}

for category, files in iac_files.items():
    print(f"\n{category}:")
    for f in files:
        print(f"  {f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 11. Custos Estimados (TP1 → TP5)

# COMMAND ----------

# MAGIC %md
# MAGIC | Serviço | Uso Acumulado | Custo Estimado/Mês | Otimização |
# MAGIC |---------|-------------|-------------------|------------|
# MAGIC | **S3** | 6 buckets (~160 MB) | ~$0.01 | Lifecycle → IA |
# MAGIC | **Glue** | 4 DBs + 1 job | ~$0.10/run | Job Bookmark (incremental) |
# MAGIC | **Lake Formation** | 4 locations + CLS | Sem custo adicional | — |
# MAGIC | **Redshift Serverless** | 128 RPU, 4 schemas, ~20 tabelas | ~$0.50/hora (quando ativo) | Auto-pause |
# MAGIC | **Step Functions** | 1 workflow, 7 states | ~$0.001/execução | — |
# MAGIC | **CodePipeline** | 1 pipeline | $1/mês (free tier) | — |
# MAGIC | **CodeBuild** | 2 projetos | ~$0.005/build | 100 min/mês grátis |
# MAGIC | **CloudWatch** | 2 alarms | $0.20/mês | — |
# MAGIC | **SNS** | 1 topic | Grátis (1K notif.) | — |
# MAGIC | **Secrets Manager** | 1 secret | $0.40/mês | — |
# MAGIC | **QuickSight** | 1 dashboard, 5 datasets | $9/user/mês (Author) | Reader: $0.30/sessão |
# MAGIC | **Databricks Compute** | Serverless notebooks + DLT | Pay-per-DBU | Serverless (sem idle) |
# MAGIC | **Databricks SQL** | Serverless warehouse | Pay-per-DBU | Auto-stop 10min |
# MAGIC | **Databricks MLflow** | 4 runs, 1 model | Incluso | — |
# MAGIC | **Model Serving** | 1 endpoint (Small) | Pay-per-request | Scale-to-zero |
# MAGIC | **Lakehouse Monitoring** | 2 monitores | Incluso (DBUs no refresh) | Triggered (não contínuo) |
# MAGIC | **AI Functions** | 1K chamadas sentiment | Pay-per-token | Amostra limitada |
# MAGIC
# MAGIC **Custo total estimado**: ~$15–25/mês (tudo serverless/pay-per-use, sem idle)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 12. Resumo Executivo TP5

# COMMAND ----------

# MAGIC %md
# MAGIC | Atividade | Entregável | Status |
# MAGIC |-----------|------------|--------|
# MAGIC | 1.1 Features ML (dbt) | `pb_gold.features_ml` — 13 features pré-agregadas | ✅ |
# MAGIC | 1.2 Consumo ML (Databricks) | Leitura Redshift → MLflow Production → inferência batch | ✅ |
# MAGIC | 1.3 Retorno ao Redshift | `pb_gold.predicoes_databricks_ml` — predições + probabilidade_falha | ✅ |
# MAGIC | 2.1 GenAI Sentiment | `ai_analyze_sentiment` em 1K reviews (DBSQL) | ✅ |
# MAGIC | 2.2 RAG Conceitual | Pipeline LLMOps documentado (chunking→embedding→VectorSearch→LLM) | ✅ |
# MAGIC | 3.1 Lakehouse Monitoring | 2 monitores (dim_clientes_rfm + predicoes_ml) + dashboard auto | ✅ |
# MAGIC | 3.2 System Tables | `system.billing.usage` — consumo DBUs por SKU/categoria | ✅ |
# MAGIC | 3.3 Tendências | Data Fabric (aplicabilidade) + Lambda vs Kappa (recomendação) | ✅ |
# MAGIC | 4.1 Dashboard QuickSight | 5 abas (KPIs + RFM + Sellers + Produtos + ML) + alertas | ✅ |
# MAGIC | 5.1 Monitoramento | Este notebook — inventário completo TP1→TP5 | ✅ |
# MAGIC
# MAGIC ### Fluxo End-to-End Completo (TP1 → TP5)
# MAGIC ```
# MAGIC CSV (Olist) → S3 Raw → Glue → S3 Bronze → Databricks (DLT) → Silver
# MAGIC   → Spark-Redshift → raw_databricks → dbt staging → dbt marts (Gold)
# MAGIC     → features_ml → Databricks (MLflow Production) → inferência
# MAGIC       → predicoes_databricks_ml → QuickSight Dashboard
# MAGIC         → Alerta probabilidade_falha → Ação operacional
# MAGIC
# MAGIC Paralelo:
# MAGIC   Silver reviews → ai_analyze_sentiment → sentimento GenAI
# MAGIC   Gold tables → Lakehouse Monitoring → dashboard qualidade
# MAGIC   System Tables → análise de consumo DBUs
# MAGIC   Unity Catalog → governança (PII, CLS, RLS, linhagem)
# MAGIC ```
