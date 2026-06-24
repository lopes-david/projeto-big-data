# Databricks notebook source
# MAGIC %md
# MAGIC # TP5 — Atividade 1.2: Feedback Loop — Consumo ML no Databricks
# MAGIC
# MAGIC ## Objetivo
# MAGIC Fechar o ciclo do dado (Feedback Loop):
# MAGIC 1. Ler a tabela `features_ml` do **Redshift** (criada pelo dbt no TP5 1.1)
# MAGIC 2. Carregar o modelo de predição de atraso do **MLflow Model Registry** (Production)
# MAGIC 3. Fazer o **score (inferência)** dos dados
# MAGIC
# MAGIC ## Contexto de Negócio (TP1 — Questão 3, Diretor de Operações)
# MAGIC > "Não temos um modelo que nos diga quais pedidos têm risco de atraso logo após
# MAGIC > a aprovação. Isso nos custou R$ 2,3M em reembolsos no ano passado."
# MAGIC
# MAGIC ## Fluxo do Feedback Loop
# MAGIC ```
# MAGIC Redshift (pb_gold.features_ml)
# MAGIC   → Databricks (leitura via Spark-Redshift)
# MAGIC     → MLflow Model Registry (Production)
# MAGIC       → Inferência (score batch)
# MAGIC         → DataFrame com predições + probabilidades
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração — Conexão Redshift

# COMMAND ----------

REDSHIFT_HOST = "default-workgroup.234828142988.sa-east-1.redshift-serverless.amazonaws.com"
REDSHIFT_PORT = "5439"
REDSHIFT_DB = "dev"
REDSHIFT_USER = dbutils.secrets.get(scope="pb-brasilmart", key="redshift-user")
REDSHIFT_PASS = dbutils.secrets.get(scope="pb-brasilmart", key="redshift-password")

REDSHIFT_URL = f"jdbc:redshift://{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}"

S3_STAGING = "s3://pb-silver-brasilmart-234828142988/redshift-staging"
IAM_ROLE = "arn:aws:iam::234828142988:role/redshift-s3-copy-role"

print(f"Redshift: {REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}")
print(f"S3 Staging: {S3_STAGING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leitura da Tabela `features_ml` do Redshift
# MAGIC
# MAGIC A tabela `pb_gold.features_ml` foi criada pelo dbt (TP5 1.1) com as 13 features
# MAGIC pré-agregadas no nível de pedido, replicando a mesma feature engineering
# MAGIC do modelo treinado no TP4.

# COMMAND ----------

REDSHIFT_SCHEMA = "pb_gold"
REDSHIFT_TABLE = "features_ml"

df_features = (
    spark.read
    .format("io.databricks.spark.redshift")
    .option("url", REDSHIFT_URL)
    .option("user", REDSHIFT_USER)
    .option("password", REDSHIFT_PASS)
    .option("dbtable", f"{REDSHIFT_SCHEMA}.{REDSHIFT_TABLE}")
    .option("tempdir", S3_STAGING)
    .option("aws_iam_role", IAM_ROLE)
    .load()
)

total = df_features.count()
print(f"Tabela {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE} lida do Redshift: {total:,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Validação dos Dados Lidos

# COMMAND ----------

from pyspark.sql import functions as F

print(f"Schema:")
df_features.printSchema()

print(f"\nDistribuição do target (label):")
df_features.groupBy("label").count().orderBy("label").show()

print(f"\nEstatísticas das features numéricas:")
df_features.select(
    "tempo_aprovacao_seg", "tempo_postagem_seg", "total_pago",
    "max_parcelas", "total_itens_valor", "total_frete",
    "peso_medio_kg", "volume_medio_cm3", "qtd_itens"
).describe().show()

print(f"\nDistribuição por customer_state (top 10):")
df_features.groupBy("customer_state").count().orderBy(F.desc("count")).show(10)

print(f"\nAmostra:")
df_features.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Carregar Modelo do MLflow Model Registry (Production)
# MAGIC
# MAGIC O modelo `pb-brasilmart-predicao-atraso` foi treinado no TP4 com
# MAGIC `sklearn.LogisticRegression` (flavor sklearn, servível via REST).

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

MODEL_NAME = "pb-brasilmart-predicao-atraso"

client = MlflowClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Transicionar Modelo para Production (se ainda não estiver)

# COMMAND ----------

all_versions = client.search_model_versions(f"name='{MODEL_NAME}'")

sklearn_version = None
for v in sorted(all_versions, key=lambda x: int(x.version), reverse=True):
    run = client.get_run(v.run_id)
    flavor_tag = run.data.tags.get("flavor", "")
    if flavor_tag == "sklearn":
        sklearn_version = v
        break

if sklearn_version is None:
    sklearn_version = max(all_versions, key=lambda x: int(x.version))

current_stage = sklearn_version.current_stage
print(f"Modelo: {MODEL_NAME}")
print(f"Versão sklearn: v{sklearn_version.version}")
print(f"Stage atual: {current_stage}")
print(f"Run ID: {sklearn_version.run_id}")

if current_stage != "Production":
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=sklearn_version.version,
        stage="Production",
        archive_existing_versions=True,
    )
    print(f"\n→ Modelo transicionado para Production!")
else:
    print(f"\n→ Modelo já está em Production.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Carregar Modelo Production

# COMMAND ----------

model_uri = f"models:/{MODEL_NAME}/Production"
model = mlflow.pyfunc.load_model(model_uri)

print(f"Modelo carregado: {model_uri}")
print(f"Tipo: {type(model)}")

model_info = model.metadata
print(f"Run ID: {model_info.run_id}")
print(f"Flavors: {list(model_info.flavors.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Preparação dos Dados para Inferência
# MAGIC
# MAGIC O modelo sklearn espera 13 features numéricas. As colunas `customer_state`
# MAGIC e `seller_state` precisam ser codificadas (LabelEncoder) da mesma forma
# MAGIC que no treinamento (TP4).

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder

pdf = df_features.toPandas()
print(f"DataFrame Pandas: {pdf.shape[0]:,} registros, {pdf.shape[1]} colunas")

le_customer = LabelEncoder()
le_seller = LabelEncoder()

pdf["customer_state_enc"] = le_customer.fit_transform(pdf["customer_state"].fillna("UNKNOWN"))
pdf["seller_state_enc"] = le_seller.fit_transform(pdf["seller_state"].fillna("UNKNOWN"))

feature_cols = [
    "tempo_aprovacao_seg",
    "tempo_postagem_seg",
    "total_pago",
    "max_parcelas",
    "pag_cartao",
    "pag_boleto",
    "total_itens_valor",
    "total_frete",
    "peso_medio_kg",
    "volume_medio_cm3",
    "qtd_itens",
    "customer_state_enc",
    "seller_state_enc",
]

numeric_cols = [c for c in feature_cols if c not in ("customer_state_enc", "seller_state_enc")]
pdf[numeric_cols] = pdf[numeric_cols].fillna(0)

X = pdf[feature_cols].astype(float)

print(f"\nFeatures para inferência: {feature_cols}")
print(f"Shape: {X.shape}")
print(f"\nEncoding customer_state: {dict(zip(le_customer.classes_, le_customer.transform(le_customer.classes_)))}")
print(f"Encoding seller_state: {dict(zip(le_seller.classes_, le_seller.transform(le_seller.classes_)))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Score (Inferência Batch)

# COMMAND ----------

predictions = model.predict(X)

pdf["predicao_atraso"] = predictions

sklearn_model = model.unwrap_python_model()
if hasattr(sklearn_model, "predict_proba"):
    probas = sklearn_model.predict_proba(X.values)
    pdf["probabilidade_atraso"] = probas[:, 1]
else:
    underlying = mlflow.sklearn.load_model(model_uri)
    probas = underlying.predict_proba(X.values)
    pdf["probabilidade_atraso"] = probas[:, 1]

pdf["risco_atraso"] = pd.cut(
    pdf["probabilidade_atraso"],
    bins=[0, 0.3, 0.6, 1.0],
    labels=["baixo", "medio", "alto"],
    include_lowest=True,
)

print(f"Inferência concluída: {len(pdf):,} pedidos classificados")
print(f"\nDistribuição das predições:")
print(pdf["predicao_atraso"].value_counts().rename({0: "no_prazo (0)", 1: "atrasado (1)"}))
print(f"\nDistribuição de risco:")
print(pdf["risco_atraso"].value_counts())
print(f"\nEstatísticas da probabilidade de atraso:")
print(pdf["probabilidade_atraso"].describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Validação — Predições vs Labels Reais

# COMMAND ----------

from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, confusion_matrix, classification_report
)

y_real = pdf["label"].values
y_pred = pdf["predicao_atraso"].values
y_prob = pdf["probabilidade_atraso"].values

acc = accuracy_score(y_real, y_pred)
prec = precision_score(y_real, y_pred, average="weighted")
rec = recall_score(y_real, y_pred, average="weighted")
f1 = f1_score(y_real, y_pred, average="weighted")
auc = roc_auc_score(y_real, y_prob)

prec_atraso = precision_score(y_real, y_pred, pos_label=1)
rec_atraso = recall_score(y_real, y_pred, pos_label=1)
f1_atraso = f1_score(y_real, y_pred, pos_label=1)

cm = confusion_matrix(y_real, y_pred)

print("=" * 65)
print("VALIDAÇÃO — Inferência Batch sobre features_ml (Redshift)")
print("=" * 65)
print(f"\nMétricas Gerais:")
print(f"  Accuracy:           {acc:.4f}")
print(f"  Precision (weighted): {prec:.4f}")
print(f"  Recall (weighted):    {rec:.4f}")
print(f"  F1-Score (weighted):  {f1:.4f}")
print(f"  AUC-ROC:              {auc:.4f}")
print(f"\nMétricas Classe 'atrasado' (label=1):")
print(f"  Precision: {prec_atraso:.4f}")
print(f"  Recall:    {rec_atraso:.4f}")
print(f"  F1-Score:  {f1_atraso:.4f}")
print(f"\nConfusion Matrix:")
print(f"{'':>20} Pred=0   Pred=1")
print(f"  Real=0 (no_prazo)  {cm[0][0]:>6}   {cm[0][1]:>6}")
print(f"  Real=1 (atrasado)  {cm[1][0]:>6}   {cm[1][1]:>6}")
print(f"\nClassification Report:")
print(classification_report(y_real, y_pred, target_names=["no_prazo", "atrasado"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Análise por Faixa de Risco

# COMMAND ----------

print("Análise por faixa de risco:")
print("-" * 55)

for risco in ["baixo", "medio", "alto"]:
    subset = pdf[pdf["risco_atraso"] == risco]
    total_faixa = len(subset)
    if total_faixa == 0:
        continue
    realmente_atrasados = subset["label"].sum()
    taxa_real = realmente_atrasados / total_faixa * 100
    prob_media = subset["probabilidade_atraso"].mean()
    ticket_medio = subset["total_pago"].mean()

    print(f"\n  Risco {risco.upper():>5}: {total_faixa:>6,} pedidos ({total_faixa/len(pdf)*100:.1f}%)")
    print(f"    Realmente atrasados: {realmente_atrasados:>5,} ({taxa_real:.1f}%)")
    print(f"    Probabilidade média: {prob_media:.3f}")
    print(f"    Ticket médio:        R$ {ticket_medio:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Amostra de Pedidos Classificados

# COMMAND ----------

display_cols = [
    "order_id", "customer_state", "seller_state",
    "total_pago", "qtd_itens", "peso_medio_kg",
    "label", "predicao_atraso", "probabilidade_atraso", "risco_atraso"
]

print("\n--- Pedidos com MAIOR risco de atraso (top 10) ---")
top_risco = pdf.nlargest(10, "probabilidade_atraso")[display_cols]
print(top_risco.to_string(index=False))

print("\n--- Pedidos com MENOR risco de atraso (top 10) ---")
low_risco = pdf.nsmallest(10, "probabilidade_atraso")[display_cols]
print(low_risco.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Retorno ao Redshift — Tabela `predicoes_databricks_ml`
# MAGIC
# MAGIC Escreve os resultados do score de volta para o Redshift, fechando o ciclo
# MAGIC do Feedback Loop: **Redshift → Databricks (ML) → Redshift**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Preparar DataFrame de Predições

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)
from datetime import datetime

pdf_resultado = pdf[[
    "order_id",
    "customer_id",
    "customer_state",
    "seller_state",
    "total_pago",
    "total_frete",
    "qtd_itens",
    "peso_medio_kg",
    "label",
    "predicao_atraso",
    "probabilidade_atraso",
    "risco_atraso",
]].copy()

pdf_resultado.rename(columns={"probabilidade_atraso": "probabilidade_falha"}, inplace=True)

pdf_resultado["modelo_versao"] = f"v{sklearn_version.version}"
pdf_resultado["modelo_nome"] = MODEL_NAME
pdf_resultado["scored_at"] = datetime.utcnow()

pdf_resultado["risco_atraso"] = pdf_resultado["risco_atraso"].astype(str)

print(f"DataFrame de predições: {len(pdf_resultado):,} registros, {len(pdf_resultado.columns)} colunas")
print(f"\nColunas: {list(pdf_resultado.columns)}")
print(f"\nAmostra:")
print(pdf_resultado.head(5).to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Converter para Spark e Escrever no Redshift

# COMMAND ----------

df_resultado = spark.createDataFrame(pdf_resultado)

DEST_SCHEMA = "pb_gold"
DEST_TABLE = "predicoes_databricks_ml"

(df_resultado.write
 .format("io.databricks.spark.redshift")
 .option("url", REDSHIFT_URL)
 .option("user", REDSHIFT_USER)
 .option("password", REDSHIFT_PASS)
 .option("dbtable", f"{DEST_SCHEMA}.{DEST_TABLE}")
 .option("tempdir", S3_STAGING)
 .option("aws_iam_role", IAM_ROLE)
 .option("distkey", "order_id")
 .option("sortkeyspec", "SORTKEY(risco_atraso, probabilidade_falha)")
 .mode("overwrite")
 .save()
)

print(f"Tabela {DEST_SCHEMA}.{DEST_TABLE} escrita no Redshift com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Verificação — Leitura de Volta do Redshift

# COMMAND ----------

df_verificacao = (
    spark.read
    .format("io.databricks.spark.redshift")
    .option("url", REDSHIFT_URL)
    .option("user", REDSHIFT_USER)
    .option("password", REDSHIFT_PASS)
    .option("dbtable", f"{DEST_SCHEMA}.{DEST_TABLE}")
    .option("tempdir", S3_STAGING)
    .option("aws_iam_role", IAM_ROLE)
    .load()
)

rs_count = df_verificacao.count()
print(f"Verificação — registros no Redshift: {rs_count:,}")
print(f"Registros enviados:                  {len(pdf_resultado):,}")
print(f"Match: {'OK' if rs_count == len(pdf_resultado) else 'DIVERGENTE'}")

print(f"\nSchema no Redshift:")
df_verificacao.printSchema()

print(f"\nDistribuição de risco no Redshift:")
df_verificacao.groupBy("risco_atraso").count().orderBy("risco_atraso").show()

print(f"\nDistribuição predição vs label real:")
df_verificacao.groupBy("label", "predicao_atraso").count().orderBy("label", "predicao_atraso").show()

print(f"\nEstatísticas probabilidade_falha:")
df_verificacao.select("probabilidade_falha").describe().show()

print(f"\nAmostra (top 5 maior risco):")
df_verificacao.orderBy(F.desc("probabilidade_falha")).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Consulta SQL — Exemplo de Uso no Redshift
# MAGIC
# MAGIC ```sql
# MAGIC -- Pedidos com alto risco de atraso para ação proativa
# MAGIC SELECT
# MAGIC     p.order_id,
# MAGIC     p.customer_state,
# MAGIC     p.seller_state,
# MAGIC     p.total_pago,
# MAGIC     p.probabilidade_falha,
# MAGIC     p.risco_atraso,
# MAGIC     c.rfm_segment
# MAGIC FROM pb_gold.predicoes_databricks_ml p
# MAGIC LEFT JOIN pb_gold.dim_clientes_rfm c
# MAGIC     ON p.customer_id = c.customer_unique_id
# MAGIC WHERE p.risco_atraso = 'alto'
# MAGIC ORDER BY p.probabilidade_falha DESC;
# MAGIC
# MAGIC -- KPIs operacionais por faixa de risco
# MAGIC SELECT
# MAGIC     risco_atraso,
# MAGIC     COUNT(*) AS total_pedidos,
# MAGIC     ROUND(AVG(probabilidade_falha), 3) AS prob_media,
# MAGIC     ROUND(AVG(total_pago), 2) AS ticket_medio,
# MAGIC     SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END) AS realmente_atrasados,
# MAGIC     ROUND(SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END)::DECIMAL
# MAGIC           / COUNT(*) * 100, 1) AS taxa_atraso_real_pct
# MAGIC FROM pb_gold.predicoes_databricks_ml
# MAGIC GROUP BY risco_atraso
# MAGIC ORDER BY risco_atraso;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Evidências

# COMMAND ----------

print("=" * 70)
print("EVIDÊNCIAS — TP5 Atividade 1: Feedback Loop Completo")
print("=" * 70)
print(f"""
1. LEITURA DO REDSHIFT (1.2.1):
   Tabela origem: {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE}
   Registros lidos: {total:,}
   Conector: io.databricks.spark.redshift
   Staging S3: {S3_STAGING}

2. MODELO CARREGADO (1.2.2):
   Nome: {MODEL_NAME}
   URI: {model_uri}
   Versão: v{sklearn_version.version}
   Stage: Production
   Flavor: sklearn (LogisticRegression)

3. INFERÊNCIA BATCH (1.2.3):
   Total classificados: {len(pdf):,}
   Predições atrasado=1: {int(pdf['predicao_atraso'].sum()):,}
   Predições no_prazo=0: {int((pdf['predicao_atraso'] == 0).sum()):,}

4. MÉTRICAS DE VALIDAÇÃO:
   AUC-ROC:    {auc:.4f}
   Accuracy:   {acc:.4f}
   F1 (weighted): {f1:.4f}
   Precision atrasado: {prec_atraso:.4f}
   Recall atrasado:    {rec_atraso:.4f}

5. FAIXAS DE RISCO:
   Baixo:  {len(pdf[pdf['risco_atraso']=='baixo']):,} pedidos
   Médio:  {len(pdf[pdf['risco_atraso']=='medio']):,} pedidos
   Alto:   {len(pdf[pdf['risco_atraso']=='alto']):,} pedidos

6. RETORNO AO REDSHIFT (1.3):
   Tabela destino: {DEST_SCHEMA}.{DEST_TABLE}
   Registros escritos: {len(pdf_resultado):,}
   Registros verificados: {rs_count:,}
   Match: {'OK' if rs_count == len(pdf_resultado) else 'DIVERGENTE'}
   DistKey: order_id
   SortKey: risco_atraso, probabilidade_falha

7. FEEDBACK LOOP COMPLETO:
   Redshift (features_ml) → Databricks (Spark-Redshift)
     → MLflow Registry (Production) → Inferência batch
       → Redshift (predicoes_databricks_ml)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo TP5 — Atividades 1.2 e 1.3: Feedback Loop Completo
# MAGIC
# MAGIC ### 1.2.1 Leitura do Redshift
# MAGIC | Item | Detalhe |
# MAGIC |------|---------|
# MAGIC | Tabela | `pb_gold.features_ml` (criada pelo dbt — TP5 1.1) |
# MAGIC | Conector | `io.databricks.spark.redshift` (Spark-Redshift) |
# MAGIC | Staging | S3 `pb-silver-brasilmart-234828142988/redshift-staging` |
# MAGIC | Autenticação | Databricks Secrets (`pb-brasilmart` scope) |
# MAGIC
# MAGIC ### 1.2.2 Modelo MLflow Registry
# MAGIC | Item | Detalhe |
# MAGIC |------|---------|
# MAGIC | Modelo | `pb-brasilmart-predicao-atraso` |
# MAGIC | Stage | **Production** |
# MAGIC | Flavor | `sklearn` (LogisticRegression, servível) |
# MAGIC | URI | `models:/pb-brasilmart-predicao-atraso/Production` |
# MAGIC | Treinado em | TP4 (13 features, target: status_entrega) |
# MAGIC
# MAGIC ### 1.2.3 Inferência Batch
# MAGIC | Item | Detalhe |
# MAGIC |------|---------|
# MAGIC | Encoding | LabelEncoder para `customer_state` e `seller_state` |
# MAGIC | Output | `predicao_atraso` (0/1) + `probabilidade_falha` (0.0–1.0) + `risco_atraso` (baixo/medio/alto) |
# MAGIC | Validação | Métricas calculadas contra `label` real (AUC-ROC, F1, Precision, Recall) |
# MAGIC | Análise | Distribuição por faixa de risco com taxa real de atraso |
# MAGIC
# MAGIC ### 1.3 Retorno ao Redshift
# MAGIC | Item | Detalhe |
# MAGIC |------|---------|
# MAGIC | Tabela destino | `pb_gold.predicoes_databricks_ml` |
# MAGIC | Colunas | `order_id`, `customer_id`, contexto, features-chave, `predicao_atraso`, `probabilidade_falha`, `risco_atraso`, metadados do modelo |
# MAGIC | DistKey | `order_id` (JOINs com features_ml e demais tabelas Gold) |
# MAGIC | SortKey | `risco_atraso, probabilidade_falha` (filtros e ranking de risco) |
# MAGIC | Verificação | Leitura de volta + contagem cruzada |
# MAGIC
# MAGIC ### Feedback Loop Completo
# MAGIC ```
# MAGIC ┌─────────────────┐     ┌─────────────────┐     ┌──────────────────┐
# MAGIC │  Redshift        │     │  Databricks     │     │  MLflow Registry │
# MAGIC │  pb_gold.        │────→│  Spark-Redshift │────→│  Production      │
# MAGIC │  features_ml     │     │  (leitura)      │     │  sklearn model   │
# MAGIC └─────────────────┘     └────────┬────────┘     └────────┬─────────┘
# MAGIC                                  │                        │
# MAGIC                                  ▼                        ▼
# MAGIC                          ┌──────────────────────────────────────┐
# MAGIC                          │  Inferência Batch                    │
# MAGIC                          │  + predicao_atraso (0/1)             │
# MAGIC                          │  + probabilidade_falha (0.0–1.0)    │
# MAGIC                          │  + risco_atraso (baixo/medio/alto)  │
# MAGIC                          └──────────────────┬───────────────────┘
# MAGIC                                             │
# MAGIC                                             ▼
# MAGIC                          ┌──────────────────────────────────────┐
# MAGIC                          │  Redshift (escrita)                  │
# MAGIC                          │  pb_gold.predicoes_databricks_ml     │
# MAGIC                          │  → Disponível para BI e análises     │
# MAGIC                          │    prescritivas (JOINs com Gold)     │
# MAGIC                          └──────────────────────────────────────┘
# MAGIC ```
