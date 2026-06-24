# Databricks notebook source
# MAGIC %md
# MAGIC # TP4 — Atividade 3.2 / 3.3: Model Registry (sklearn) + Model Serving
# MAGIC
# MAGIC ## Objetivo
# MAGIC 1. **Registro**: Treinar com `sklearn.LogisticRegression`, registrar no MLflow
# MAGIC    Model Registry com flavor `sklearn` (servível).
# MAGIC 2. **Model Serving**: Implantar o modelo como Endpoint REST via Databricks Model Serving.
# MAGIC
# MAGIC ## Por que sklearn?
# MAGIC O notebook anterior (tp4_05) usou SparkML, que é ideal para treino distribuído
# MAGIC mas **não é suportado** pelo Databricks Model Serving. O flavor `sklearn` é
# MAGIC nativamente servível como REST endpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Preparação dos Dados (Spark → Pandas)

# COMMAND ----------

from pyspark.sql import functions as F

catalog = "pb_brasilmart"

df_orders = spark.table(f"{catalog}.silver.orders_enriched")
df_items = spark.table(f"{catalog}.silver.items_enriched")

items_agg = (
    df_items.groupBy("order_id")
    .agg(
        F.sum("total_item_value").alias("total_itens_valor"),
        F.sum("freight_value").alias("total_frete"),
        F.avg("product_weight_kg").alias("peso_medio_kg"),
        F.avg("product_volume_cm3").alias("volume_medio_cm3"),
        F.count("*").alias("qtd_itens"),
        F.first("seller_state").alias("seller_state"),
    )
)

df = (
    df_orders.alias("o")
    .join(items_agg.alias("i"), "order_id", "inner")
    .filter(F.col("o.status_entrega").isin("no_prazo", "atrasado"))
    .filter(F.col("o.tempo_aprovacao_seg").isNotNull())
    .select(
        F.when(F.col("o.status_entrega") == "atrasado", 1).otherwise(0).alias("label"),
        F.col("o.tempo_aprovacao_seg").cast("double"),
        F.col("o.tempo_postagem_seg").cast("double"),
        F.col("o.total_pago").cast("double"),
        F.col("o.max_parcelas").cast("double"),
        F.when(F.col("o.grupo_pagamento_principal") == "cartao", 1.0).otherwise(0.0).alias("pag_cartao"),
        F.when(F.col("o.grupo_pagamento_principal") == "boleto", 1.0).otherwise(0.0).alias("pag_boleto"),
        F.col("o.customer_state"),
        F.col("i.total_itens_valor").cast("double"),
        F.col("i.total_frete").cast("double"),
        F.col("i.peso_medio_kg").cast("double"),
        F.col("i.volume_medio_cm3").cast("double"),
        F.col("i.qtd_itens").cast("double"),
        F.col("i.seller_state"),
    )
)

pdf = df.toPandas()
print(f"Total registros: {len(pdf)}")
print(f"Distribuicao target:\n{pdf['label'].value_counts()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Feature Engineering (sklearn)

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder

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

le_customer = LabelEncoder()
le_seller = LabelEncoder()

pdf["customer_state_enc"] = le_customer.fit_transform(pdf["customer_state"].fillna("UNKNOWN"))
pdf["seller_state_enc"] = le_seller.fit_transform(pdf["seller_state"].fillna("UNKNOWN"))

numeric_cols = [c for c in feature_cols if c not in ("customer_state_enc", "seller_state_enc")]
pdf[numeric_cols] = pdf[numeric_cols].fillna(0)

X = pdf[feature_cols].values
y = pdf["label"].values

print(f"Shape X: {X.shape}")
print(f"Shape y: {y.shape}")
print(f"Features: {feature_cols}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Split Treino/Teste

# COMMAND ----------

from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"Treino: {X_train.shape[0]} ({y_train.sum()} atrasados = {y_train.mean()*100:.1f}%)")
print(f"Teste:  {X_test.shape[0]} ({y_test.sum()} atrasados = {y_test.mean()*100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Treinamento + Registro com MLflow (flavor sklearn)

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, average_precision_score, confusion_matrix,
    classification_report
)

experiment_name = "/Users/david.lopes@al.infnet.edu.br/pb-brasilmart-predicao-atraso"
mlflow.set_experiment(experiment_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Treinar e logar com flavor sklearn

# COMMAND ----------

with mlflow.start_run(run_name="sklearn_logistic_regression_final") as run:

    # --- Hiperparametros ---
    C = 1.0
    max_iter = 200
    solver = "lbfgs"
    class_weight = "balanced"
    penalty = "l2"

    mlflow.log_param("model_type", "sklearn.LogisticRegression")
    mlflow.log_param("flavor", "sklearn")
    mlflow.log_param("C", C)
    mlflow.log_param("max_iter", max_iter)
    mlflow.log_param("solver", solver)
    mlflow.log_param("penalty", penalty)
    mlflow.log_param("class_weight", class_weight)
    mlflow.log_param("features", feature_cols)
    mlflow.log_param("num_features", len(feature_cols))
    mlflow.log_param("train_size", X_train.shape[0])
    mlflow.log_param("test_size", X_test.shape[0])
    mlflow.log_param("target", "status_entrega (atrasado=1, no_prazo=0)")
    mlflow.log_param("dataset", "pb_brasilmart.silver.orders_enriched + items_enriched")
    mlflow.log_param("split_ratio", "80/20")
    mlflow.log_param("seed", 42)

    # --- Treinar ---
    model = LogisticRegression(
        C=C,
        max_iter=max_iter,
        solver=solver,
        class_weight=class_weight,
        penalty=penalty,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    # --- Predicao ---
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    # --- Metricas ---
    acc = accuracy_score(y_test, y_pred)
    prec = precision_score(y_test, y_pred, average="weighted")
    rec = recall_score(y_test, y_pred, average="weighted")
    f1 = f1_score(y_test, y_pred, average="weighted")
    auc_roc = roc_auc_score(y_test, y_prob)
    auc_pr = average_precision_score(y_test, y_prob)

    prec_atraso = precision_score(y_test, y_pred, pos_label=1)
    rec_atraso = recall_score(y_test, y_pred, pos_label=1)
    f1_atraso = f1_score(y_test, y_pred, pos_label=1)

    mlflow.log_metric("accuracy", acc)
    mlflow.log_metric("precision_weighted", prec)
    mlflow.log_metric("recall_weighted", rec)
    mlflow.log_metric("f1_weighted", f1)
    mlflow.log_metric("auc_roc", auc_roc)
    mlflow.log_metric("auc_pr", auc_pr)
    mlflow.log_metric("precision_atrasado", prec_atraso)
    mlflow.log_metric("recall_atrasado", rec_atraso)
    mlflow.log_metric("f1_atrasado", f1_atraso)

    # --- Feature importances ---
    coefs = model.coef_[0]
    importance = sorted(zip(feature_cols, coefs), key=lambda x: abs(x[1]), reverse=True)

    importance_text = "Feature Importances (sklearn coeficientes):\n"
    importance_text += "-" * 55 + "\n"
    for feat, coef in importance:
        importance_text += f"  {feat:30s} {coef:+.6f}\n"
        mlflow.log_metric(f"coef_{feat}", coef)
    mlflow.log_text(importance_text, "feature_importances.txt")

    # --- Confusion matrix ---
    cm = confusion_matrix(y_test, y_pred)
    cm_text = "Confusion Matrix (sklearn):\n"
    cm_text += f"{'':>20} Pred=0   Pred=1\n"
    cm_text += f"  Real=0 (no_prazo)  {cm[0][0]:>6}   {cm[0][1]:>6}\n"
    cm_text += f"  Real=1 (atrasado)  {cm[1][0]:>6}   {cm[1][1]:>6}\n"
    mlflow.log_text(cm_text, "confusion_matrix.txt")

    # --- Classification report ---
    report = classification_report(y_test, y_pred, target_names=["no_prazo", "atrasado"])
    mlflow.log_text(report, "classification_report.txt")

    # --- Input example para Model Serving ---
    input_example = pd.DataFrame([X_test[0]], columns=feature_cols)

    # --- Log do modelo com flavor sklearn ---
    mlflow.sklearn.log_model(
        sk_model=model,
        artifact_path="modelo_sklearn",
        input_example=input_example,
        registered_model_name="pb-brasilmart-predicao-atraso",
    )

    # --- Tags ---
    mlflow.set_tag("projeto", "pb-brasilmart")
    mlflow.set_tag("tp", "tp4")
    mlflow.set_tag("flavor", "sklearn")
    mlflow.set_tag("servable", "true")
    mlflow.set_tag("problema_negocio", "predicao_atraso_entrega")
    mlflow.set_tag("requisito_tp1", "Questao 3 - Diretor de Operacoes")

    run_id = run.info.run_id

    print(f"Run ID: {run_id}")
    print(f"Flavor: sklearn (servivel via Model Serving)")
    print(f"\nMetricas Gerais:")
    print(f"  AUC-ROC:            {auc_roc:.4f}")
    print(f"  AUC-PR:             {auc_pr:.4f}")
    print(f"  Accuracy:           {acc:.4f}")
    print(f"  Precision (weighted): {prec:.4f}")
    print(f"  Recall (weighted):    {rec:.4f}")
    print(f"  F1-Score (weighted):  {f1:.4f}")
    print(f"\nMetricas Classe 'atrasado':")
    print(f"  Precision: {prec_atraso:.4f}")
    print(f"  Recall:    {rec_atraso:.4f}")
    print(f"  F1-Score:  {f1_atraso:.4f}")
    print(f"\n{importance_text}")
    print(f"\n{cm_text}")
    print(f"\nClassification Report:\n{report}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificar Registro no Model Registry

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()

model_name = "pb-brasilmart-predicao-atraso"
model_details = client.get_registered_model(model_name)

print(f"Modelo: {model_details.name}")
print(f"Descricao: {model_details.description}")
print(f"Tags: {model_details.tags}")
print(f"\nVersoes:")

for mv in client.search_model_versions(f"name='{model_name}'"):
    print(f"  v{mv.version}: run_id={mv.run_id}, status={mv.status}, stage={mv.current_stage}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Atualizar versão sklearn para Production

# COMMAND ----------

latest_versions = client.get_latest_versions(model_name)
sklearn_version = None

for v in latest_versions:
    run = client.get_run(v.run_id)
    flavor_tag = run.data.tags.get("flavor", "")
    if flavor_tag == "sklearn":
        sklearn_version = v.version
        break

if sklearn_version is None:
    all_versions = client.search_model_versions(f"name='{model_name}'")
    sklearn_version = max(v.version for v in all_versions)

client.update_model_version(
    name=model_name,
    version=sklearn_version,
    description=(
        "Regressao Logistica (sklearn) para predicao de atraso na entrega. "
        f"Flavor: sklearn | AUC-ROC: {auc_roc:.4f} | F1: {f1:.4f} | "
        f"Recall atrasado: {rec_atraso:.4f}. "
        "Servivel via Databricks Model Serving (REST endpoint). "
        "Requisito TP1 Q3: Diretor de Operacoes."
    )
)

client.set_model_version_tag(model_name, sklearn_version, "validation_status", "aprovado")
client.set_model_version_tag(model_name, sklearn_version, "flavor", "sklearn")
client.set_model_version_tag(model_name, sklearn_version, "servable", "true")

print(f"Modelo {model_name} v{sklearn_version} atualizado e pronto para serving.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Databricks Model Serving — Endpoint REST
# MAGIC
# MAGIC ### Arquitetura
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────┐
# MAGIC │              Databricks Model Serving                           │
# MAGIC ├──────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                  │
# MAGIC │  MLflow Model Registry                                          │
# MAGIC │  └─ pb-brasilmart-predicao-atraso (v latest, sklearn)           │
# MAGIC │                    │                                             │
# MAGIC │                    ▼                                             │
# MAGIC │  Serving Endpoint: pb-brasilmart-atraso-endpoint                │
# MAGIC │  ┌──────────────────────────────────────────────────────────┐   │
# MAGIC │  │  POST /serving-endpoints/.../invocations                │   │
# MAGIC │  │  Content-Type: application/json                         │   │
# MAGIC │  │                                                          │   │
# MAGIC │  │  Request:                                                │   │
# MAGIC │  │  {"dataframe_records": [                                 │   │
# MAGIC │  │    {"tempo_aprovacao_seg": 600,                          │   │
# MAGIC │  │     "peso_medio_kg": 2.5, ...}                          │   │
# MAGIC │  │  ]}                                                      │   │
# MAGIC │  │                                                          │   │
# MAGIC │  │  Response:                                               │   │
# MAGIC │  │  {"predictions": [0]}  ← 0=no_prazo, 1=atrasado         │   │
# MAGIC │  └──────────────────────────────────────────────────────────┘   │
# MAGIC │                                                                  │
# MAGIC └──────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Criar Serving Endpoint via API

# COMMAND ----------

import requests
import json

databricks_host = spark.conf.get("spark.databricks.workspaceUrl")
databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

endpoint_name = "pb-brasilmart-atraso-endpoint"

serving_config = {
    "name": endpoint_name,
    "config": {
        "served_entities": [
            {
                "entity_name": model_name,
                "entity_version": str(sklearn_version),
                "workload_size": "Small",
                "scale_to_zero_enabled": True,
            }
        ],
        "traffic_config": {
            "routes": [
                {
                    "served_model_name": f"{model_name}-{sklearn_version}",
                    "traffic_percentage": 100,
                }
            ]
        },
    },
    "tags": [
        {"key": "projeto", "value": "pb-brasilmart"},
        {"key": "tp", "value": "tp4"},
        {"key": "problema", "value": "predicao_atraso"},
    ],
}

headers = {
    "Authorization": f"Bearer {databricks_token}",
    "Content-Type": "application/json",
}

response = requests.post(
    f"https://{databricks_host}/api/2.0/serving-endpoints",
    headers=headers,
    json=serving_config,
)

if response.status_code == 200:
    result = response.json()
    print(f"Endpoint criado com sucesso!")
    print(f"  Nome:   {endpoint_name}")
    print(f"  Estado: {result.get('state', {}).get('ready', 'CREATING')}")
    print(f"  URL:    https://{databricks_host}/serving-endpoints/{endpoint_name}/invocations")
elif response.status_code == 409:
    print(f"Endpoint '{endpoint_name}' ja existe. Atualizando configuracao...")
    response_update = requests.put(
        f"https://{databricks_host}/api/2.0/serving-endpoints/{endpoint_name}/config",
        headers=headers,
        json=serving_config["config"],
    )
    if response_update.status_code == 200:
        print("  Endpoint atualizado com sucesso!")
    else:
        print(f"  Erro ao atualizar: {response_update.status_code} — {response_update.text}")
else:
    print(f"Erro ao criar endpoint: {response.status_code}")
    print(response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Verificar Status do Endpoint

# COMMAND ----------

import time

print(f"Aguardando endpoint '{endpoint_name}' ficar pronto...")
for attempt in range(1, 31):
    response = requests.get(
        f"https://{databricks_host}/api/2.0/serving-endpoints/{endpoint_name}",
        headers=headers,
    )
    if response.status_code == 200:
        state = response.json().get("state", {})
        ready = state.get("ready", "NOT_READY")
        print(f"  [{attempt}/30] Estado: {ready}")
        if ready == "READY":
            print(f"\nEndpoint PRONTO!")
            print(f"  URL: https://{databricks_host}/serving-endpoints/{endpoint_name}/invocations")
            break
    time.sleep(20)
else:
    print("Timeout — verifique o status no Console: Serving → Endpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Testar o Endpoint (Inferência REST)

# COMMAND ----------

test_payload = {
    "dataframe_records": [
        {
            "tempo_aprovacao_seg": 600.0,
            "tempo_postagem_seg": 86400.0,
            "total_pago": 150.0,
            "max_parcelas": 3.0,
            "pag_cartao": 1.0,
            "pag_boleto": 0.0,
            "total_itens_valor": 130.0,
            "total_frete": 20.0,
            "peso_medio_kg": 1.5,
            "volume_medio_cm3": 5000.0,
            "qtd_itens": 2.0,
            "customer_state_enc": 25.0,
            "seller_state_enc": 25.0,
        },
        {
            "tempo_aprovacao_seg": 3600.0,
            "tempo_postagem_seg": 259200.0,
            "total_pago": 450.0,
            "max_parcelas": 10.0,
            "pag_cartao": 0.0,
            "pag_boleto": 1.0,
            "total_itens_valor": 400.0,
            "total_frete": 50.0,
            "peso_medio_kg": 15.0,
            "volume_medio_cm3": 80000.0,
            "qtd_itens": 1.0,
            "customer_state_enc": 0.0,
            "seller_state_enc": 10.0,
        }
    ]
}

response = requests.post(
    f"https://{databricks_host}/serving-endpoints/{endpoint_name}/invocations",
    headers=headers,
    json=test_payload,
)

if response.status_code == 200:
    predictions = response.json()
    print("Inferencia via REST — Sucesso!")
    print(f"\nPayload enviado: 2 pedidos")
    print(f"Resposta: {json.dumps(predictions, indent=2)}")

    preds = predictions.get("predictions", predictions)
    labels = {0: "no_prazo", 1: "atrasado"}
    print(f"\nInterpretacao:")
    for i, p in enumerate(preds if isinstance(preds, list) else [preds]):
        pred_val = p if isinstance(p, (int, float)) else p.get("prediction", p)
        print(f"  Pedido {i+1}: {labels.get(int(pred_val), pred_val)}")
else:
    print(f"Erro na inferencia: {response.status_code}")
    print(response.text)
    print("\nNOTA: Se o endpoint ainda esta inicializando, aguarde e tente novamente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.4 Exemplo de chamada externa (curl)
# MAGIC
# MAGIC ```bash
# MAGIC # Obter token do Databricks
# MAGIC DATABRICKS_TOKEN="dapi..."
# MAGIC DATABRICKS_HOST="dbc-5f5c64be-0b42.cloud.databricks.com"
# MAGIC
# MAGIC # Inferencia: pedido com risco de atraso?
# MAGIC curl -X POST \
# MAGIC   "https://${DATABRICKS_HOST}/serving-endpoints/pb-brasilmart-atraso-endpoint/invocations" \
# MAGIC   -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d '{
# MAGIC     "dataframe_records": [{
# MAGIC       "tempo_aprovacao_seg": 3600.0,
# MAGIC       "tempo_postagem_seg": 259200.0,
# MAGIC       "total_pago": 450.0,
# MAGIC       "max_parcelas": 10.0,
# MAGIC       "pag_cartao": 0.0,
# MAGIC       "pag_boleto": 1.0,
# MAGIC       "total_itens_valor": 400.0,
# MAGIC       "total_frete": 50.0,
# MAGIC       "peso_medio_kg": 15.0,
# MAGIC       "volume_medio_cm3": 80000.0,
# MAGIC       "qtd_itens": 1.0,
# MAGIC       "customer_state_enc": 0.0,
# MAGIC       "seller_state_enc": 10.0
# MAGIC     }]
# MAGIC   }'
# MAGIC
# MAGIC # Resposta esperada:
# MAGIC # {"predictions": [1]}  ← 1 = atrasado (alto risco de atraso)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Evidências

# COMMAND ----------

print("=" * 70)
print("EVIDENCIAS — Model Registry + Model Serving")
print("=" * 70)
print(f"""
1. MODEL REGISTRY (Console → Models):
   Modelo: {model_name}
   Versao sklearn: v{sklearn_version}
   Flavor: sklearn
   Tags: flavor=sklearn, servable=true, validation_status=aprovado

2. SERVING ENDPOINT (Console → Serving):
   Endpoint: {endpoint_name}
   Modelo servido: {model_name} v{sklearn_version}
   Workload: Small (scale-to-zero habilitado)
   URL: https://{databricks_host}/serving-endpoints/{endpoint_name}/invocations

3. MLFLOW EXPERIMENT:
   Experiment: {experiment_name}
   Runs: 4 total (3 SparkML + 1 sklearn)
   Run sklearn: {run_id}

4. ARTEFATOS LOGADOS:
   - modelo_sklearn/ (sklearn LogisticRegression serializado)
   - feature_importances.txt
   - confusion_matrix.txt
   - classification_report.txt
   - input_example.json (para referencia do payload)

5. METRICAS (sklearn final):
   AUC-ROC:           {auc_roc:.4f}
   AUC-PR:            {auc_pr:.4f}
   Accuracy:          {acc:.4f}
   F1 (weighted):     {f1:.4f}
   Precision atrasado: {prec_atraso:.4f}
   Recall atrasado:    {rec_atraso:.4f}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo TP4 — Atividades 3.2 e 3.3
# MAGIC
# MAGIC ### 3.2 Registro no Model Registry (sklearn)
# MAGIC | Item | Detalhe |
# MAGIC |------|---------|
# MAGIC | Modelo | `sklearn.LogisticRegression` (C=1.0, solver=lbfgs, balanced) |
# MAGIC | Flavor | `sklearn` (via `mlflow.sklearn.log_model`) |
# MAGIC | Registry | `pb-brasilmart-predicao-atraso` |
# MAGIC | Input Example | DataFrame com 13 features (salvo como artefato) |
# MAGIC | Diferença do tp4_05 | SparkML não é servível; sklearn é nativamente suportado |
# MAGIC
# MAGIC ### 3.3 Model Serving (Endpoint REST)
# MAGIC | Item | Detalhe |
# MAGIC |------|---------|
# MAGIC | Endpoint | `pb-brasilmart-atraso-endpoint` |
# MAGIC | Workload | Small, scale-to-zero habilitado |
# MAGIC | Formato request | `{"dataframe_records": [{...features...}]}` |
# MAGIC | Formato response | `{"predictions": [0]}` (0=no_prazo, 1=atrasado) |
# MAGIC | Autenticação | Bearer token (Databricks PAT) |
# MAGIC | Criação | API REST `POST /api/2.0/serving-endpoints` |
# MAGIC
# MAGIC ### Fluxo completo MLOps
# MAGIC ```
# MAGIC Dados Silver → Feature Engineering → sklearn.fit()
# MAGIC     → MLflow Tracking (params + metrics + artifacts)
# MAGIC         → Model Registry (pb-brasilmart-predicao-atraso)
# MAGIC             → Model Serving (REST endpoint)
# MAGIC                 → POST /invocations → {"predictions": [0/1]}
# MAGIC ```
