# Databricks notebook source
# MAGIC %md
# MAGIC # TP4 — Monitoramento de Recursos AWS e Databricks
# MAGIC
# MAGIC **Objetivo:** Evidenciar o uso de todos os recursos configurados no TP4:
# MAGIC CI/CD, Governança (PII, Column-Level, Row-Level Security), MLOps (MLflow,
# MAGIC Model Registry, Model Serving) e Monitoramento (CloudWatch + SNS).

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

print(f"Relatorio TP4 gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Catalogo: pb_brasilmart")
print(f"Workspace: dbc-5f5c64be-0b42.cloud.databricks.com")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. CI/CD — CodePipeline + CodeBuild + DABs

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 AWS CodePipeline
# MAGIC
# MAGIC | Recurso | Configuração |
# MAGIC |---------|-------------|
# MAGIC | **Pipeline** | `pb-brasilmart-cicd` |
# MAGIC | **Source** | GitHub (lopes-david/tp1, branch main) via CodeStar Connection |
# MAGIC | **Stage Deploy_Infra** | CodeBuild `pb-brasilmart-infra-build` → Terraform apply |
# MAGIC | **Stage Build_dbt** | CodeBuild `pb-brasilmart-dbt-build` → dbt run + test |
# MAGIC | **Artefatos** | S3: `pb-brasilmart-codepipeline-artifacts-234828142988` |
# MAGIC
# MAGIC ### 1.2 CodeBuild Projects
# MAGIC
# MAGIC | Projeto | Buildspec | Runtime | O que executa |
# MAGIC |---------|-----------|---------|---------------|
# MAGIC | `pb-brasilmart-dbt-build` | `buildspec_dbt.yml` | Python 3.11 | dbt deps → run → test → docs |
# MAGIC | `pb-brasilmart-infra-build` | `buildspec_infra.yml` | Terraform 1.9 | init → validate → plan → apply |
# MAGIC
# MAGIC ### 1.3 Databricks Asset Bundle (DABs)
# MAGIC
# MAGIC | Recurso | Configuração |
# MAGIC |---------|-------------|
# MAGIC | **Bundle** | `pb-brasilmart` (`infra/databricks/bundle.yml`) |
# MAGIC | **Target dev** | mode=development, root=/Users/.bundle/dev |
# MAGIC | **Target prod** | mode=production, run_as=david.lopes@al.infnet.edu.br |
# MAGIC | **Pipelines** | pb-brasilmart-silver, pb-brasilmart-silver-cdc |
# MAGIC | **Job** | pb-brasilmart-silver-to-gold (DLT → Validação → Gold) |

# COMMAND ----------

print("=== Arquivos CI/CD no repositório ===")
cicd_files = {
    "infra/aws/codepipeline/pipeline.json": "Definição do CodePipeline",
    "infra/aws/codepipeline/buildspec_dbt.yml": "Build dbt (deps → run → test)",
    "infra/aws/codepipeline/buildspec_infra.yml": "Build Terraform (plan → apply)",
    "infra/databricks/bundle.yml": "Databricks Asset Bundle (targets dev/prod)",
    "infra/terraform/codepipeline.tf": "Terraform: IAM + CodeBuild + CodePipeline",
}
for path, desc in cicd_files.items():
    print(f"  {path:<55} {desc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Monitoramento e Alertas — CloudWatch + SNS

# COMMAND ----------

# MAGIC %md
# MAGIC | Recurso | Configuração |
# MAGIC |---------|-------------|
# MAGIC | **SNS Topic** | `pb-brasilmart-alertas` (sa-east-1) |
# MAGIC | **Inscrição** | `david.lopes@al.infnet.edu.br` (protocolo e-mail) |
# MAGIC | **Alarm 1** | `pb-brasilmart-stepfunctions-falha` — ExecutionsFailed >= 1 em 5min |
# MAGIC | **Alarm 2** | `pb-brasilmart-stepfunctions-timeout` — ExecutionTime > 30min |
# MAGIC | **Ação** | ALARM → Publica no SNS → E-mail automático |
# MAGIC
# MAGIC ### Arquivos
# MAGIC | Arquivo | Tipo |
# MAGIC |---------|------|
# MAGIC | `infra/aws/setup_monitoring.sh` | Script AWS CLI |
# MAGIC | `infra/terraform/monitoring.tf` | Terraform (SNS + CloudWatch) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Governança — Descoberta PII

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Colunas PII Identificadas (Glue Data Catalog + Unity Catalog)

# COMMAND ----------

pii_inventory = [
    ("customers",   "customer_id",              "IDENTIFICADOR_DIRETO",  "CPF_HASH"),
    ("customers",   "customer_unique_id",       "IDENTIFICADOR_DIRETO",  "ID_PESSOAL"),
    ("customers",   "customer_zip_code_prefix",  "QUASI_IDENTIFICADOR",  "CEP"),
    ("customers",   "customer_city",            "QUASI_IDENTIFICADOR",   "LOCALIZACAO"),
    ("sellers",     "seller_id",                "IDENTIFICADOR_DIRETO",  "CNPJ_CPF_HASH"),
    ("sellers",     "seller_zip_code_prefix",    "QUASI_IDENTIFICADOR",  "CEP"),
    ("sellers",     "seller_city",              "QUASI_IDENTIFICADOR",   "LOCALIZACAO"),
    ("geolocation", "geolocation_lat",          "QUASI_IDENTIFICADOR",   "COORDENADA_GPS"),
    ("geolocation", "geolocation_lng",          "QUASI_IDENTIFICADOR",   "COORDENADA_GPS"),
    ("geolocation", "geolocation_zip_code_prefix","QUASI_IDENTIFICADOR", "CEP"),
    ("reviews",     "review_comment_title",     "TEXTO_LIVRE",           "CONTEUDO_USUARIO"),
    ("reviews",     "review_comment_message",   "TEXTO_LIVRE",           "CONTEUDO_USUARIO"),
    ("orders",      "customer_id",              "IDENTIFICADOR_INDIRETO","CHAVE_ESTRANGEIRA"),
]

print(f"{'Tabela':<15} {'Coluna':<30} {'Tipo PII':<25} {'Categoria'}")
print("-" * 90)
for tab, col, tipo, cat in pii_inventory:
    print(f"{tab:<15} {col:<30} {tipo:<25} {cat}")
print(f"\nTotal: {len(pii_inventory)} colunas PII em 5 tabelas")
print("Metodo: Tagging Glue Data Catalog (simulação Macie) + ALTER COLUMN COMMENT no Unity Catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Governança — Column-Level Security (Lake Formation)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Permissões do Analista Jr.
# MAGIC
# MAGIC **IAM Role**: `pb-brasilmart-analista-jr` (ARN: arn:aws:iam::234828142988:role/pb-brasilmart-analista-jr)
# MAGIC
# MAGIC | Tabela | Colunas BLOQUEADAS | Mecanismo |
# MAGIC |--------|--------------------|-----------|
# MAGIC | customers (bronze+silver) | `customer_id`, `customer_unique_id` | ColumnWildcard + ExcludedColumnNames |
# MAGIC | sellers (bronze+silver) | `seller_id` | ColumnWildcard + ExcludedColumnNames |
# MAGIC | geolocation (bronze) | `geolocation_lat`, `geolocation_lng` | ColumnWildcard + ExcludedColumnNames |
# MAGIC | reviews (bronze) | `review_comment_title`, `review_comment_message` | ColumnWildcard + ExcludedColumnNames |
# MAGIC | orders, items, payments, products | *(nenhuma)* | Full SELECT |
# MAGIC | Gold (todas) | *(nenhuma)* | Full SELECT + DESCRIBE |
# MAGIC
# MAGIC **Verificação Athena:**
# MAGIC ```sql
# MAGIC -- Funciona (colunas não-PII):
# MAGIC SELECT customer_zip_code_prefix, customer_city FROM pb_bronze_brasilmart.customers;
# MAGIC -- FALHA (coluna PII bloqueada):
# MAGIC SELECT customer_id FROM pb_bronze_brasilmart.customers;
# MAGIC → AccessDeniedException: Insufficient Lake Formation permission(s)
# MAGIC ```

# COMMAND ----------

print("=== Arquivos Column-Level Security ===")
cls_files = {
    "infra/aws/setup_column_security.sh": "Script AWS CLI (IAM Role + Lake Formation permissions)",
    "infra/terraform/column_security.tf": "Terraform (IAM + Lake Formation column-level)",
    "infra/aws/setup_pii_tagging.sh": "Script de tagging PII no Glue Data Catalog",
}
for path, desc in cls_files.items():
    print(f"  {path:<55} {desc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Governança — Row-Level Security (Unity Catalog)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar tabelas com Row Filter ativo
# MAGIC SELECT
# MAGIC   table_catalog,
# MAGIC   table_schema,
# MAGIC   table_name,
# MAGIC   row_filter
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_catalog = 'pb_brasilmart'
# MAGIC   AND row_filter IS NOT NULL
# MAGIC ORDER BY table_schema, table_name;

# COMMAND ----------

print("=== Row-Level Security — Funcoes de Filtro ===")
rls_config = [
    ("silver.orders_enriched", "filtro_regiao_norte",           "customer_state"),
    ("silver.customers",       "filtro_regiao_norte_customers", "customer_state"),
    ("silver.sellers",         "filtro_regiao_norte_sellers",   "seller_state"),
]
print(f"{'Tabela':<30} {'Funcao':<40} {'Coluna Filtrada'}")
print("-" * 90)
for tab, func, col in rls_config:
    print(f"{tab:<30} {func:<40} {col}")

print(f"\nGrupo: Regiao_Norte")
print(f"Filtro: customer_state IN ('AM','PA','AC','RO','RR','AP','TO')")
print(f"Admin: ve todas as linhas (IS_ACCOUNT_GROUP_MEMBER retorna FALSE)")

# COMMAND ----------

# Evidencia: contagem por regiao (visao admin vs Regiao_Norte)
total = spark.table("pb_brasilmart.silver.orders_enriched").count()
norte = spark.sql("""
    SELECT COUNT(*) AS total FROM pb_brasilmart.silver.orders_enriched
    WHERE customer_state IN ('AM','PA','AC','RO','RR','AP','TO')
""").first()["total"]

print(f"\nEvidencia RLS:")
print(f"  Admin (todas regioes):  {total:,} pedidos")
print(f"  Regiao_Norte (filtrado): {norte:,} pedidos ({norte/total*100:.2f}%)")
print(f"  Regioes bloqueadas:      {total - norte:,} pedidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. MLOps — MLflow Tracking + Model Registry

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

experiment_name = "/Users/david.lopes@al.infnet.edu.br/pb-brasilmart-predicao-atraso"
client = MlflowClient()

try:
    experiment = client.get_experiment_by_name(experiment_name)
    print("=== MLflow Experiment ===")
    print(f"  Nome:          {experiment.name}")
    print(f"  Experiment ID: {experiment.experiment_id}")
    print(f"  Lifecycle:     {experiment.lifecycle_stage}")
    print(f"  Artifact URI:  {experiment.artifact_location}")

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["start_time DESC"]
    )
    print(f"\n  Total de runs: {len(runs)}")
    print(f"\n  {'Run Name':<40} {'Flavor':<10} {'AUC-ROC':>10} {'F1':>10} {'Status':<12}")
    print("  " + "-" * 90)
    for r in runs:
        name = r.info.run_name or r.info.run_id[:8]
        flavor = r.data.tags.get("flavor", "spark")
        auc = r.data.metrics.get("auc_roc", 0)
        f1_val = r.data.metrics.get("f1_score", r.data.metrics.get("f1_weighted", 0))
        status = r.info.status
        print(f"  {name:<40} {flavor:<10} {auc:>10.4f} {f1_val:>10.4f} {status:<12}")
except Exception as e:
    print(f"  Experiment nao encontrado (execute tp4_05 e tp4_06 primeiro): {str(e)[:60]}")

# COMMAND ----------

# Model Registry
model_name = "pb-brasilmart-predicao-atraso"
print(f"\n=== MLflow Model Registry ===")
try:
    model = client.get_registered_model(model_name)
    print(f"  Modelo:    {model.name}")
    print(f"  Tags:      {model.tags}")

    versions = client.search_model_versions(f"name='{model_name}'")
    print(f"  Versoes:   {len(versions)}")
    print(f"\n  {'Versao':<10} {'Stage':<15} {'Run ID':<35} {'Status':<12}")
    print("  " + "-" * 75)
    for v in versions:
        print(f"  v{v.version:<9} {v.current_stage:<15} {v.run_id:<35} {v.status:<12}")
except Exception as e:
    print(f"  Modelo nao registrado ainda: {str(e)[:60]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. MLOps — Model Serving (Endpoint REST)

# COMMAND ----------

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
        config = ep.get("config", {})
        served = config.get("served_entities", [{}])

        print("=== Model Serving Endpoint ===")
        print(f"  Nome:        {ep.get('name')}")
        print(f"  Estado:      {state.get('ready', 'UNKNOWN')}")
        print(f"  URL:         https://{databricks_host}/serving-endpoints/{endpoint_name}/invocations")
        print(f"  Criado em:   {ep.get('creation_timestamp', 'N/A')}")

        if served:
            e = served[0]
            print(f"\n  Modelo servido:")
            print(f"    Entity:    {e.get('entity_name', 'N/A')}")
            print(f"    Version:   {e.get('entity_version', 'N/A')}")
            print(f"    Workload:  {e.get('workload_size', 'N/A')}")
            print(f"    Scale-0:   {e.get('scale_to_zero_enabled', 'N/A')}")
    elif resp.status_code == 404:
        print(f"  Endpoint '{endpoint_name}' nao encontrado (execute tp4_06 primeiro)")
    else:
        print(f"  Erro ao consultar endpoint: {resp.status_code}")
except Exception as e:
    print(f"  Nao foi possivel verificar o endpoint: {str(e)[:60]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Databricks — Unity Catalog (estado completo)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG pb_brasilmart;

# COMMAND ----------

print("=== Unity Catalog — Inventario Completo ===\n")
total_tables = 0
for schema_name in ["bronze", "silver", "gold"]:
    try:
        tables = spark.sql(f"SHOW TABLES IN pb_brasilmart.{schema_name}").collect()
        total_tables += len(tables)
        print(f"pb_brasilmart.{schema_name}: {len(tables)} tabelas")
        for t in tables:
            try:
                detail = spark.sql(f"DESCRIBE DETAIL pb_brasilmart.{schema_name}.{t.tableName}").first()
                size_mb = detail.sizeInBytes / 1024 / 1024
                count = spark.table(f"pb_brasilmart.{schema_name}.{t.tableName}").count()
                print(f"  {t.tableName:<35} {count:>10,} rows  {size_mb:>8.2f} MB")
            except:
                print(f"  {t.tableName:<35} (detalhes indisponiveis)")
        print()
    except Exception as e:
        print(f"pb_brasilmart.{schema_name}: ERRO — {str(e)[:50]}")

print(f"Total geral: {total_tables} tabelas no Unity Catalog")

# COMMAND ----------

# Funcoes de filtro RLS
print("=== Unity Catalog — Funcoes SQL ===")
try:
    funcs = spark.sql("SHOW FUNCTIONS IN pb_brasilmart.silver LIKE 'filtro_*'").collect()
    for f in funcs:
        print(f"  {f[0]}")
    if not funcs:
        print("  (nenhuma funcao de filtro encontrada — execute tp4_04 primeiro)")
except:
    print("  (nao foi possivel listar funcoes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. AWS — Recursos Provisionados (TP4)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.1 Novos Recursos AWS criados no TP4
# MAGIC
# MAGIC | Serviço | Recurso | Configuração |
# MAGIC |---------|---------|-------------|
# MAGIC | **CodePipeline** | `pb-brasilmart-cicd` | Source(GitHub) → Infra(TF) → dbt(run+test) |
# MAGIC | **CodeBuild** | `pb-brasilmart-dbt-build` | Python 3.11, dbt-redshift |
# MAGIC | **CodeBuild** | `pb-brasilmart-infra-build` | Terraform 1.9 |
# MAGIC | **S3** | `pb-brasilmart-codepipeline-artifacts-*` | Artefatos do pipeline |
# MAGIC | **IAM Role** | `CodePipelineServiceRole-pb-brasilmart` | CodePipeline + S3 + CodeBuild |
# MAGIC | **IAM Role** | `CodeBuildServiceRole-pb-brasilmart` | Logs + S3 + SecretsManager + Redshift |
# MAGIC | **IAM Role** | `pb-brasilmart-analista-jr` | Athena + Glue (read) + S3 (silver/gold) |
# MAGIC | **Secrets Manager** | `pb-brasilmart/redshift` | host, user, password |
# MAGIC | **SNS Topic** | `pb-brasilmart-alertas` | E-mail: david.lopes@al.infnet.edu.br |
# MAGIC | **CloudWatch Alarm** | `pb-brasilmart-stepfunctions-falha` | ExecutionsFailed >= 1 → SNS |
# MAGIC | **CloudWatch Alarm** | `pb-brasilmart-stepfunctions-timeout` | ExecutionTime > 30min → SNS |
# MAGIC | **Lake Formation** | Column-Level Permissions | 4 tabelas com ExcludedColumnNames |
# MAGIC | **Glue Catalog** | PII Tags | 5 tabelas, 13 colunas taggeadas |
# MAGIC
# MAGIC ### 9.2 Recursos AWS existentes (TP1–TP3)
# MAGIC
# MAGIC | Serviço | Recurso |
# MAGIC |---------|---------|
# MAGIC | **S3** | 4 buckets (raw, bronze, silver, gold) — lifecycle, versioning, SSE-S3 |
# MAGIC | **Glue** | 4 databases, 1 job batch, 1 crawler |
# MAGIC | **Lake Formation** | Admin root, 4 locations, GlueETLRole |
# MAGIC | **Redshift** | Serverless 128 RPU, 4 schemas, 9 tabelas raw_databricks |
# MAGIC | **Step Functions** | pb-brasilmart-orchestration (7 states) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 10. Custos e Otimizações (TP4)

# COMMAND ----------

# MAGIC %md
# MAGIC | Serviço | Uso TP4 | Custo Estimado | Otimização |
# MAGIC |---------|---------|----------------|------------|
# MAGIC | **S3** | +1 bucket artefatos | ~$0.01/mês | Lifecycle automático |
# MAGIC | **CodePipeline** | 1 pipeline | $1/mês (free tier: 1 pipeline grátis) | — |
# MAGIC | **CodeBuild** | 2 projetos | ~$0.005/build (100 min/mês grátis) | Builds sob demanda |
# MAGIC | **CloudWatch** | 2 alarms | $0.20/mês (2 × $0.10) | Apenas métricas necessárias |
# MAGIC | **SNS** | 1 topic, 1 sub | Grátis (primeiras 1000 notif.) | — |
# MAGIC | **Lake Formation** | Column-level | Sem custo adicional | — |
# MAGIC | **Secrets Manager** | 1 secret | $0.40/mês | Rotação automática |
# MAGIC | **Databricks MLflow** | 4 runs, 1 model | Incluso no workspace | — |
# MAGIC | **Model Serving** | 1 endpoint (Small) | Pay-per-request + scale-to-zero | Auto-scale down |
# MAGIC | **IAM** | 3 roles | Sem custo | Princípio do menor privilégio |
# MAGIC
# MAGIC **Total estimado TP4**: ~$2/mês (tudo serverless/pay-per-use)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 11. Inventário Completo (TP1 → TP4)

# COMMAND ----------

print("=== INVENTARIO: Notebooks Databricks ===")
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
        "PB-TP4-05_mlops_mlflow (MLflow Tracking — 3 runs SparkML)",
        "PB-TP4-06_model_registry_serving (sklearn + Model Serving)",
        "PB-TP4-07_monitoramento (este notebook)",
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

print("\n=== INVENTARIO: Infraestrutura como Código ===")
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
        "infra/aws/setup_s3.sh — Setup inicial dos buckets",
        "infra/aws/setup_lake_formation.sh — Lake Formation + Glue",
        "infra/aws/setup_monitoring.sh — CloudWatch + SNS (TP4)",
        "infra/aws/setup_pii_tagging.sh — PII tags no Glue (TP4)",
        "infra/aws/setup_column_security.sh — Column-Level Security (TP4)",
    ],
    "Orquestração": [
        "infra/aws/step_functions_workflow.json — Step Functions",
        "infra/aws/codepipeline/pipeline.json — CodePipeline (TP4)",
        "infra/aws/codepipeline/buildspec_dbt.yml — Build dbt (TP4)",
        "infra/aws/codepipeline/buildspec_infra.yml — Build Terraform (TP4)",
        "infra/databricks/workflow_silver_gold.json — Databricks Workflow",
        "infra/databricks/bundle.yml — Databricks Asset Bundle (TP4)",
    ],
    "dbt": [
        "dbt/pb_brasilmart/profiles.yml — Redshift (dev + prod)",
        "dbt/pb_brasilmart/dbt_project.yml — Config do projeto",
        "dbt/pb_brasilmart/models/staging/ — 7 views (Silver)",
        "dbt/pb_brasilmart/models/marts/ — 4 tabelas (Gold)",
    ],
}

for category, files in iac_files.items():
    print(f"\n{category}:")
    for f in files:
        print(f"  {f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 12. Resumo Executivo TP4
# MAGIC
# MAGIC | Atividade | Entregável | Status |
# MAGIC |-----------|------------|--------|
# MAGIC | 1.1 CI/CD AWS | CodePipeline + CodeBuild (dbt + Terraform) | ✅ |
# MAGIC | 1.2 CD Databricks | Asset Bundle (bundle.yml) com targets dev/prod | ✅ |
# MAGIC | 1.3 Monitoramento | CloudWatch Alarm → SNS → e-mail | ✅ |
# MAGIC | 2.1 Descoberta PII | 13 colunas taggeadas (Glue + Unity Catalog) | ✅ |
# MAGIC | 2.2 Column-Level Security | Lake Formation ExcludedColumnNames (Analista Jr.) | ✅ |
# MAGIC | 2.3 Row-Level Security | Unity Catalog SET ROW FILTER (Regiao_Norte) | ✅ |
# MAGIC | 2.4 Linhagem de Dados | tempo_total_seg + total_item_value → gmv (Bronze→Gold) | ✅ |
# MAGIC | 3.1 MLflow Tracking | 3 runs SparkML + 1 run sklearn, métricas e artefatos | ✅ |
# MAGIC | 3.2 Model Registry | sklearn flavor, tags, descrição, versionamento | ✅ |
# MAGIC | 3.3 Model Serving | Endpoint REST (scale-to-zero, Small) | ✅ |
# MAGIC | 4.1 Monitoramento | Este notebook — evidências de todos os recursos | ✅ |
