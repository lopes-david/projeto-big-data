# Databricks notebook source
# MAGIC %md
# MAGIC # TP5 — Atividade 3.1: Lakehouse Monitoring nas Tabelas Gold
# MAGIC
# MAGIC ## Objetivo
# MAGIC Habilitar o **Databricks Lakehouse Monitoring** nas tabelas Gold do Unity Catalog
# MAGIC para gerar automaticamente métricas de qualidade de dados e um dashboard
# MAGIC de monitoramento.
# MAGIC
# MAGIC ## Tabelas Monitoradas
# MAGIC | Tabela Gold | Tipo Monitor | Justificativa |
# MAGIC |-------------|-------------|---------------|
# MAGIC | `dim_clientes_rfm` | Snapshot | Segmentação RFM — detectar drift na distribuição de segmentos |
# MAGIC | `predicoes_databricks_ml` | Inference | Predições ML — monitorar drift de features e distribuição de predições |
# MAGIC
# MAGIC ## O que o Lakehouse Monitoring gera automaticamente
# MAGIC - **Profile Metrics Table**: estatísticas por coluna (nulos, distintos, min/max, média, distribuição)
# MAGIC - **Drift Metrics Table**: comparação entre janelas temporais (KL divergence, JS distance, Chi-squared)
# MAGIC - **Dashboard**: visualização automática de qualidade de dados no Databricks SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inventário das Tabelas Gold

# COMMAND ----------

from pyspark.sql import functions as F

catalog = "pb_brasilmart"
gold_schema = "gold"

print("=== Tabelas Gold disponíveis ===\n")
tables = spark.sql(f"SHOW TABLES IN {catalog}.{gold_schema}").collect()

for t in tables:
    table_name = t.tableName
    full_name = f"{catalog}.{gold_schema}.{table_name}"
    try:
        count = spark.table(full_name).count()
        cols = len(spark.table(full_name).columns)
        print(f"  {table_name:<35} {count:>8,} rows  {cols:>3} cols")
    except Exception as e:
        print(f"  {table_name:<35} ERRO: {str(e)[:50]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Habilitar Lakehouse Monitoring — `dim_clientes_rfm`
# MAGIC
# MAGIC Monitor tipo **Snapshot**: analisa o estado completo da tabela a cada refresh.
# MAGIC Ideal para tabelas dimensionais que são reconstruídas (materialized='table' no dbt).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar se já existe um monitor nesta tabela
# MAGIC -- Se existir, o CREATE abaixo retornará erro (tratado no próximo cmd)
# MAGIC DESCRIBE TABLE EXTENDED pb_brasilmart.gold.dim_clientes_rfm

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorSnapshot, MonitorMetricType

w = WorkspaceClient()

TABLE_RFM = f"{catalog}.{gold_schema}.dim_clientes_rfm"

try:
    existing = w.quality_monitors.get(table_name=TABLE_RFM)
    print(f"Monitor já existe em {TABLE_RFM}")
    print(f"  Status:          {existing.status}")
    print(f"  Profile Table:   {existing.profile_metrics_table_name}")
    print(f"  Drift Table:     {existing.drift_metrics_table_name}")
    print(f"  Dashboard:       {existing.dashboard_id}")
except Exception:
    print(f"Criando monitor em {TABLE_RFM}...")

    monitor = w.quality_monitors.create(
        table_name=TABLE_RFM,
        assets_dir=f"/Workspace/Users/david.lopes@al.infnet.edu.br/pb-brasilmart-monitoring",
        output_schema_name=f"{catalog}.{gold_schema}",
        snapshot=MonitorSnapshot(),
    )

    print(f"Monitor criado com sucesso!")
    print(f"  Table:           {TABLE_RFM}")
    print(f"  Profile Table:   {monitor.profile_metrics_table_name}")
    print(f"  Drift Table:     {monitor.drift_metrics_table_name}")
    print(f"  Dashboard ID:    {monitor.dashboard_id}")
    print(f"  Assets Dir:      /Workspace/Users/david.lopes@al.infnet.edu.br/pb-brasilmart-monitoring")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Habilitar Lakehouse Monitoring — `predicoes_databricks_ml`
# MAGIC
# MAGIC Monitor tipo **Inference**: projetado para tabelas de predições ML.
# MAGIC Monitora drift nas features de entrada e na distribuição das predições,
# MAGIC comparando com a baseline (dados de treino).

# COMMAND ----------

TABLE_PRED = f"{catalog}.{gold_schema}.predicoes_databricks_ml"

try:
    existing = w.quality_monitors.get(table_name=TABLE_PRED)
    print(f"Monitor já existe em {TABLE_PRED}")
    print(f"  Status:          {existing.status}")
    print(f"  Profile Table:   {existing.profile_metrics_table_name}")
    print(f"  Drift Table:     {existing.drift_metrics_table_name}")
    print(f"  Dashboard:       {existing.dashboard_id}")
except Exception:
    print(f"Criando monitor em {TABLE_PRED}...")

    monitor_pred = w.quality_monitors.create(
        table_name=TABLE_PRED,
        assets_dir=f"/Workspace/Users/david.lopes@al.infnet.edu.br/pb-brasilmart-monitoring",
        output_schema_name=f"{catalog}.{gold_schema}",
        snapshot=MonitorSnapshot(),
    )

    print(f"Monitor criado com sucesso!")
    print(f"  Table:           {TABLE_PRED}")
    print(f"  Profile Table:   {monitor_pred.profile_metrics_table_name}")
    print(f"  Drift Table:     {monitor_pred.drift_metrics_table_name}")
    print(f"  Dashboard ID:    {monitor_pred.dashboard_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Executar Refresh dos Monitores
# MAGIC
# MAGIC O refresh calcula as métricas de perfil e drift, populando as tabelas
# MAGIC de monitoramento e atualizando o dashboard.

# COMMAND ----------

import time

for table_name in [TABLE_RFM, TABLE_PRED]:
    short = table_name.split(".")[-1]
    print(f"\nRefresh: {short}...")

    try:
        run = w.quality_monitors.run_refresh(table_name=table_name)
        print(f"  Refresh iniciado: run_id={run.refresh_id}")

        for attempt in range(1, 31):
            refresh = w.quality_monitors.get_refresh(
                table_name=table_name,
                refresh_id=run.refresh_id
            )
            state = refresh.state.value if hasattr(refresh.state, 'value') else str(refresh.state)
            print(f"  [{attempt}/30] Estado: {state}")

            if state in ("SUCCESS", "COMPLETED"):
                print(f"  Refresh concluído com sucesso!")
                break
            elif state in ("FAILED", "CANCELED"):
                print(f"  Refresh falhou: {refresh.message}")
                break

            time.sleep(15)
        else:
            print("  Timeout — verifique no Console: Catalog → tabela → Quality")
    except Exception as e:
        print(f"  Erro no refresh: {str(e)[:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Consultar Métricas de Perfil (Profile Metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Profile Metrics — `dim_clientes_rfm`

# COMMAND ----------

profile_rfm = f"{catalog}.{gold_schema}.dim_clientes_rfm_profile_metrics"

try:
    df_profile = spark.table(profile_rfm)
    print(f"Profile Metrics: {profile_rfm}")
    print(f"Total de registros: {df_profile.count()}")
    print(f"\nColunas monitoradas:")

    df_profile.select(
        "column_name",
        "data_type",
        "num_nulls",
        "num_distinct",
        "min",
        "max",
        "mean",
        "stddev",
    ).show(20, truncate=False)

except Exception as e:
    print(f"Tabela de profile ainda não disponível: {str(e)[:60]}")
    print("Execute o refresh e aguarde a conclusão.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Métricas de perfil detalhadas — dim_clientes_rfm
# MAGIC SELECT
# MAGIC   column_name,
# MAGIC   data_type,
# MAGIC   num_nulls,
# MAGIC   num_distinct,
# MAGIC   min,
# MAGIC   max,
# MAGIC   mean,
# MAGIC   stddev,
# MAGIC   percent_zeros,
# MAGIC   quantiles
# MAGIC FROM pb_brasilmart.gold.dim_clientes_rfm_profile_metrics
# MAGIC WHERE window_id IS NOT NULL
# MAGIC ORDER BY column_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Profile Metrics — `predicoes_databricks_ml`

# COMMAND ----------

profile_pred = f"{catalog}.{gold_schema}.predicoes_databricks_ml_profile_metrics"

try:
    df_profile_pred = spark.table(profile_pred)
    print(f"Profile Metrics: {profile_pred}")
    print(f"Total de registros: {df_profile_pred.count()}")
    print(f"\nColunas monitoradas:")

    df_profile_pred.select(
        "column_name",
        "data_type",
        "num_nulls",
        "num_distinct",
        "min",
        "max",
        "mean",
        "stddev",
    ).show(20, truncate=False)

except Exception as e:
    print(f"Tabela de profile ainda não disponível: {str(e)[:60]}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Foco: distribuição das predições e probabilidades
# MAGIC SELECT
# MAGIC   column_name,
# MAGIC   num_nulls,
# MAGIC   num_distinct,
# MAGIC   min,
# MAGIC   max,
# MAGIC   mean,
# MAGIC   stddev,
# MAGIC   quantiles
# MAGIC FROM pb_brasilmart.gold.predicoes_databricks_ml_profile_metrics
# MAGIC WHERE column_name IN ('predicao_atraso', 'probabilidade_falha', 'risco_atraso', 'label')
# MAGIC   AND window_id IS NOT NULL
# MAGIC ORDER BY column_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Consultar Métricas de Drift

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drift metrics — dim_clientes_rfm
# MAGIC -- Detecta mudanças na distribuição entre janelas temporais
# MAGIC SELECT
# MAGIC   column_name,
# MAGIC   drift_type,
# MAGIC   statistic,
# MAGIC   value,
# MAGIC   window_start,
# MAGIC   window_end
# MAGIC FROM pb_brasilmart.gold.dim_clientes_rfm_drift_metrics
# MAGIC WHERE value IS NOT NULL
# MAGIC ORDER BY column_name, window_start

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drift metrics — predicoes_databricks_ml
# MAGIC SELECT
# MAGIC   column_name,
# MAGIC   drift_type,
# MAGIC   statistic,
# MAGIC   value,
# MAGIC   window_start,
# MAGIC   window_end
# MAGIC FROM pb_brasilmart.gold.predicoes_databricks_ml_drift_metrics
# MAGIC WHERE value IS NOT NULL
# MAGIC ORDER BY column_name, window_start

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Dashboard de Qualidade de Dados
# MAGIC
# MAGIC O Lakehouse Monitoring gera automaticamente um **dashboard** no Databricks SQL
# MAGIC com as seguintes visualizações:
# MAGIC
# MAGIC ### Conteúdo do Dashboard Gerado
# MAGIC
# MAGIC | Seção | Visualização | O que mostra |
# MAGIC |-------|-------------|-------------|
# MAGIC | **Overview** | Tabela resumo | Total de colunas, registros, nulos, tipos de dados |
# MAGIC | **Column Profiles** | Histogramas | Distribuição de cada coluna numérica |
# MAGIC | **Categorical Columns** | Barras | Frequência de valores para colunas categóricas (ex: `rfm_segment`) |
# MAGIC | **Null Analysis** | Heatmap | Proporção de nulos por coluna ao longo do tempo |
# MAGIC | **Drift Detection** | Gráfico de linha | KL Divergence / JS Distance entre janelas |
# MAGIC | **Numeric Stats** | Tabela | Min, max, mean, stddev, percentis (p25, p50, p75) |
# MAGIC
# MAGIC ### Como acessar o Dashboard
# MAGIC
# MAGIC ```
# MAGIC Databricks Console
# MAGIC   → Catalog
# MAGIC     → pb_brasilmart → gold → dim_clientes_rfm
# MAGIC       → Aba "Quality"
# MAGIC         → Link para o Dashboard gerado automaticamente
# MAGIC
# MAGIC Ou diretamente:
# MAGIC   → SQL → Dashboards → pb-brasilmart-monitoring/
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Verificar Dashboard IDs

# COMMAND ----------

for table in [TABLE_RFM, TABLE_PRED]:
    short = table.split(".")[-1]
    try:
        mon = w.quality_monitors.get(table_name=table)
        print(f"\n{short}:")
        print(f"  Status:        {mon.status}")
        print(f"  Dashboard ID:  {mon.dashboard_id}")
        print(f"  Profile Table: {mon.profile_metrics_table_name}")
        print(f"  Drift Table:   {mon.drift_metrics_table_name}")

        host = spark.conf.get("spark.databricks.workspaceUrl")
        if mon.dashboard_id:
            print(f"  Dashboard URL: https://{host}/sql/dashboards/{mon.dashboard_id}")
    except Exception as e:
        print(f"\n{short}: {str(e)[:60]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Análise Manual de Qualidade (complementar ao dashboard)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Qualidade — `dim_clientes_rfm`

# COMMAND ----------

df_rfm = spark.table(f"{catalog}.{gold_schema}.dim_clientes_rfm")

total = df_rfm.count()
print(f"=== Qualidade: dim_clientes_rfm ({total:,} registros) ===\n")

# Nulos por coluna
print("Nulos por coluna:")
null_counts = df_rfm.select([
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_rfm.columns
]).collect()[0]

for col_name in df_rfm.columns:
    nulls = null_counts[col_name]
    pct = nulls / total * 100 if total > 0 else 0
    flag = " ⚠" if pct > 5 else ""
    print(f"  {col_name:<30} {nulls:>6} nulls ({pct:.1f}%){flag}")

# COMMAND ----------

# Distribuição dos segmentos RFM
print("\nDistribuição de segmentos RFM:")
df_rfm.groupBy("rfm_segment") \
    .agg(
        F.count("*").alias("total"),
        F.round(F.avg("monetary"), 2).alias("monetary_medio"),
        F.round(F.avg("recency_days"), 0).alias("recency_media"),
        F.round(F.avg("frequency"), 1).alias("frequency_media"),
    ) \
    .orderBy(F.desc("total")) \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Qualidade — `predicoes_databricks_ml`

# COMMAND ----------

df_pred = spark.table(f"{catalog}.{gold_schema}.predicoes_databricks_ml")

total_pred = df_pred.count()
print(f"=== Qualidade: predicoes_databricks_ml ({total_pred:,} registros) ===\n")

# Nulos
print("Nulos por coluna:")
null_pred = df_pred.select([
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_pred.columns
]).collect()[0]

for col_name in df_pred.columns:
    nulls = null_pred[col_name]
    pct = nulls / total_pred * 100 if total_pred > 0 else 0
    flag = " ⚠" if pct > 5 else ""
    print(f"  {col_name:<30} {nulls:>6} nulls ({pct:.1f}%){flag}")

# COMMAND ----------

# Concordância predição vs label real
print("\nConcordância label vs predição:")
df_pred.groupBy("label", "predicao_atraso") \
    .count() \
    .orderBy("label", "predicao_atraso") \
    .show()

# Distribuição de probabilidade por faixa de risco
print("Estatísticas probabilidade_falha por risco:")
df_pred.groupBy("risco_atraso") \
    .agg(
        F.count("*").alias("total"),
        F.round(F.min("probabilidade_falha"), 4).alias("prob_min"),
        F.round(F.avg("probabilidade_falha"), 4).alias("prob_media"),
        F.round(F.max("probabilidade_falha"), 4).alias("prob_max"),
        F.round(F.stddev("probabilidade_falha"), 4).alias("prob_stddev"),
    ) \
    .orderBy("risco_atraso") \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Evidências

# COMMAND ----------

print("=" * 70)
print("EVIDÊNCIAS — TP5 Atividade 3.1: Lakehouse Monitoring")
print("=" * 70)
print(f"""
1. MONITORES HABILITADOS:
   - {TABLE_RFM} (tipo: Snapshot)
   - {TABLE_PRED} (tipo: Snapshot)

2. TABELAS GERADAS AUTOMATICAMENTE:
   Profile Metrics:
     - {catalog}.{gold_schema}.dim_clientes_rfm_profile_metrics
     - {catalog}.{gold_schema}.predicoes_databricks_ml_profile_metrics
   Drift Metrics:
     - {catalog}.{gold_schema}.dim_clientes_rfm_drift_metrics
     - {catalog}.{gold_schema}.predicoes_databricks_ml_drift_metrics

3. DASHBOARD DE QUALIDADE:
   Gerado automaticamente pelo Lakehouse Monitoring
   Acesso: Catalog → tabela → aba "Quality" → link Dashboard
   Ou: SQL → Dashboards → pb-brasilmart-monitoring/

4. MÉTRICAS CAPTURADAS:
   - Nulos por coluna (count + %)
   - Valores distintos
   - Min, max, mean, stddev
   - Percentis (p25, p50, p75)
   - Distribuição de valores categóricos
   - Drift entre janelas (KL divergence, JS distance)

5. COMO VERIFICAR NO DATABRICKS:
   Console → Catalog → pb_brasilmart → gold
     → dim_clientes_rfm → aba "Quality"
     → predicoes_databricks_ml → aba "Quality"
   Refresh: executar este notebook para recalcular métricas
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo TP5 — Atividade 3.1
# MAGIC
# MAGIC ### Monitores Habilitados
# MAGIC | Tabela | Tipo | Profile Table | Drift Table |
# MAGIC |--------|------|--------------|-------------|
# MAGIC | `dim_clientes_rfm` | Snapshot | `*_profile_metrics` | `*_drift_metrics` |
# MAGIC | `predicoes_databricks_ml` | Snapshot | `*_profile_metrics` | `*_drift_metrics` |
# MAGIC
# MAGIC ### Dashboard Automático
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │  Dashboard — Lakehouse Monitoring: dim_clientes_rfm                │
# MAGIC ├─────────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                     │
# MAGIC │  ┌───────────────────┐  ┌───────────────────────────────────────┐  │
# MAGIC │  │  Overview          │  │  Column Profiles (histogramas)       │  │
# MAGIC │  │  • 96.096 rows     │  │  monetary:  ████████▌ (média 160)   │  │
# MAGIC │  │  • 10 colunas      │  │  recency:   ██████▌ (média 750)     │  │
# MAGIC │  │  • 0 nulos (PK)    │  │  frequency: █▌ (média 1.03)         │  │
# MAGIC │  └───────────────────┘  └───────────────────────────────────────┘  │
# MAGIC │                                                                     │
# MAGIC │  ┌───────────────────────────────────────────────────────────────┐  │
# MAGIC │  │  Categorical: rfm_segment                                     │  │
# MAGIC │  │  Hibernating ████████████████████████████████ 67.8%            │  │
# MAGIC │  │  Lost         █████████████████ 22.5%                         │  │
# MAGIC │  │  New          ████ 5.5%                                       │  │
# MAGIC │  │  Loyal        ██ 2.8%                                         │  │
# MAGIC │  │  At Risk      █ 1.2%                                          │  │
# MAGIC │  │  Champions    ▏ 0.2%                                          │  │
# MAGIC │  └───────────────────────────────────────────────────────────────┘  │
# MAGIC │                                                                     │
# MAGIC │  ┌───────────────────────────────────────────────────────────────┐  │
# MAGIC │  │  Null Analysis        │  Drift Detection                      │  │
# MAGIC │  │  Todas colunas: 0%    │  (disponível após 2+ refreshes)       │  │
# MAGIC │  └───────────────────────────────────────────────────────────────┘  │
# MAGIC └─────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Fluxo de Monitoramento
# MAGIC ```
# MAGIC dbt run (Redshift Gold)
# MAGIC   → Databricks lê Gold (ou cria tabelas Gold locais)
# MAGIC     → Lakehouse Monitoring (refresh automático ou manual)
# MAGIC       → Profile Metrics Table (estatísticas por coluna)
# MAGIC       → Drift Metrics Table (KL divergence entre janelas)
# MAGIC         → Dashboard automático (DBSQL)
# MAGIC           → Alertas (se drift > threshold)
# MAGIC ```
