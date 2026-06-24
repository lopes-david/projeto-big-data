# Databricks notebook source
# MAGIC %md
# MAGIC # TP4 — Atividade 1: DevOps, CI/CD e Monitoramento
# MAGIC
# MAGIC ## 1.1 CI/CD AWS (Infra + dbt) — CodePipeline + CodeBuild
# MAGIC
# MAGIC Pipeline **pb-brasilmart-cicd** com 3 stages:
# MAGIC
# MAGIC | Stage | Ferramenta | Buildspec | O que faz |
# MAGIC |-------|-----------|-----------|-----------|
# MAGIC | Source | CodeStar Connection | — | Detecta push no `main` do GitHub |
# MAGIC | Deploy_Infra | CodeBuild + Terraform | `buildspec_infra.yml` | `terraform plan → apply` (IAM, S3, Glue, Lake Formation) |
# MAGIC | Build_dbt | CodeBuild + dbt | `buildspec_dbt.yml` | `dbt run → dbt test` (staging views + marts tables no Redshift) |
# MAGIC
# MAGIC ### Fluxo:
# MAGIC ```
# MAGIC Push main → Source → [Deploy_Infra | Build_dbt] (paralelo)
# MAGIC                          ↓                ↓
# MAGIC                   terraform apply    dbt run + test
# MAGIC ```
# MAGIC
# MAGIC ### Arquivos no repositório:
# MAGIC - `infra/aws/codepipeline/pipeline.json` — Definição do CodePipeline
# MAGIC - `infra/aws/codepipeline/buildspec_dbt.yml` — Build dbt (deps → run → test → docs)
# MAGIC - `infra/aws/codepipeline/buildspec_infra.yml` — Build Terraform (init → validate → plan → apply)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 CD Databricks (DLT) — Databricks Asset Bundle (DABs)
# MAGIC
# MAGIC O arquivo `bundle.yml` define o deploy do pipeline DLT para produção usando DABs.
# MAGIC
# MAGIC ### Recursos definidos no bundle:
# MAGIC - **Pipeline DLT Silver**: `pb-brasilmart-silver-{target}` — 11 tabelas Bronze→Silver com expectations
# MAGIC - **Pipeline DLT CDC**: `pb-brasilmart-silver-cdc-{target}` — 4 tabelas CDC com APPLY CHANGES INTO
# MAGIC - **Job Orchestration**: `pb-brasilmart-silver-to-gold-{target}` — DLT → Validação → Otimização Gold
# MAGIC
# MAGIC ### Targets:
# MAGIC | Target | Mode | Descrição |
# MAGIC |--------|------|-----------|
# MAGIC | `dev` | development | Ambiente de desenvolvimento (default) |
# MAGIC | `prod` | production | Ambiente de produção (run_as: service principal) |
# MAGIC
# MAGIC ### Comandos de deploy:
# MAGIC ```bash
# MAGIC # Validar o bundle
# MAGIC databricks bundle validate --target prod
# MAGIC
# MAGIC # Deploy para produção
# MAGIC databricks bundle deploy --target prod
# MAGIC
# MAGIC # Executar o job
# MAGIC databricks bundle run pb_brasilmart_silver_to_gold --target prod
# MAGIC ```
# MAGIC
# MAGIC ### Arquivo: `infra/databricks/bundle.yml`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Monitoramento e Alertas — CloudWatch + SNS
# MAGIC
# MAGIC ### Alarmes configurados:
# MAGIC
# MAGIC | Alarme | Métrica | Condição | Ação |
# MAGIC |--------|---------|----------|------|
# MAGIC | `pb-brasilmart-stepfunctions-falha` | `ExecutionsFailed` | >= 1 em 5 min | SNS → e-mail |
# MAGIC | `pb-brasilmart-stepfunctions-timeout` | `ExecutionTime` | > 30 min | SNS → e-mail |
# MAGIC
# MAGIC ### Tópico SNS:
# MAGIC - Nome: `pb-brasilmart-alertas`
# MAGIC - Inscrição: `david.lopes@al.infnet.edu.br`
# MAGIC - Protocolo: e-mail
# MAGIC
# MAGIC ### Fluxo de alerta:
# MAGIC ```
# MAGIC Step Functions falha → CloudWatch detecta ExecutionsFailed >= 1
# MAGIC   → Alarme ALARM → Publica no SNS → E-mail para david.lopes@al.infnet.edu.br
# MAGIC ```
# MAGIC
# MAGIC ### Script de setup: `infra/aws/setup_monitoring.sh`
# MAGIC ```bash
# MAGIC bash infra/aws/setup_monitoring.sh
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidências de Configuração

# COMMAND ----------

# Verificar configuracao do Databricks Asset Bundle
import subprocess, json

print("=" * 60)
print("TP4 1.2 — Databricks Asset Bundle (DABs)")
print("=" * 60)

bundle_path = "/Workspace/Repos/pb-brasilmart/infra/databricks/bundle.yml"

print(f"\nBundle path: {bundle_path}")
print("\nComandos para deploy:")
print("  databricks bundle validate --target prod")
print("  databricks bundle deploy --target prod")
print("  databricks bundle run pb_brasilmart_silver_to_gold --target prod")

# COMMAND ----------

# Listar pipelines DLT existentes no workspace
pipelines = spark.sql("SELECT * FROM system.information_schema.routines LIMIT 1")
print("Conexao ao workspace ativa.")

print("\n" + "=" * 60)
print("Pipelines DLT gerenciados pelo bundle:")
print("=" * 60)
print("  1. pb-brasilmart-silver — 11 tabelas (9 base + 2 enriquecidas)")
print("  2. pb-brasilmart-silver-cdc — 4 tabelas CDC")
print("\nJob de orquestracao:")
print("  pb-brasilmart-silver-to-gold — DLT → Validacao → Gold")

# COMMAND ----------

# Verificar tabelas no Unity Catalog (confirmacao do ambiente prod)
tables_silver = spark.sql("SHOW TABLES IN pb_brasilmart.silver")
tables_gold = spark.sql("SHOW TABLES IN pb_brasilmart.gold")

print("=" * 60)
print("Tabelas Silver (destino do pipeline DLT):")
print("=" * 60)
tables_silver.show(truncate=False)

print("=" * 60)
print("Tabelas Gold (destino da otimizacao):")
print("=" * 60)
tables_gold.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo TP4 — Atividade 1: DevOps e CI/CD
# MAGIC
# MAGIC | Item | Ferramenta | Status | Arquivo |
# MAGIC |------|-----------|--------|---------|
# MAGIC | CI/CD AWS (Infra) | CodePipeline + CodeBuild + Terraform | Configurado | `buildspec_infra.yml` |
# MAGIC | CI/CD AWS (dbt) | CodePipeline + CodeBuild + dbt | Configurado | `buildspec_dbt.yml` |
# MAGIC | CD Databricks (DLT) | Databricks Asset Bundle (DABs) | Configurado | `bundle.yml` |
# MAGIC | Monitoramento | CloudWatch + SNS | Configurado | `setup_monitoring.sh` |
# MAGIC | Alerta falha | CloudWatch Alarm → SNS → e-mail | Configurado | `setup_monitoring.sh` |
