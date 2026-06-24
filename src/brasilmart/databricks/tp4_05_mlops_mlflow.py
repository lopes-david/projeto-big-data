# Databricks notebook source
# MAGIC %md
# MAGIC # TP4 — Atividade 3: MLOps Base — Rastreamento e Registro com MLflow
# MAGIC
# MAGIC ## Objetivo
# MAGIC Treinar um modelo de **Regressão Logística** para classificar se a entrega
# MAGIC ultrapassou o prazo estimado (`status_entrega = atrasado`), usando features
# MAGIC disponíveis no momento da aprovação do pedido.
# MAGIC
# MAGIC ## Contexto de Negócio (TP1 — Questão 3, Diretor de Operações)
# MAGIC > "Não temos um modelo que nos diga quais pedidos têm risco de atraso logo após
# MAGIC > a aprovação. Isso nos custou R$ 2,3M em reembolsos no ano passado."
# MAGIC
# MAGIC **Requisito TP1:** Feature engineering com `delta_entrega` + modelo preditivo de atraso.
# MAGIC
# MAGIC ## MLflow Tracking
# MAGIC - **Experiment**: `pb-brasilmart-predicao-atraso`
# MAGIC - **Hiperparâmetros**: C, max_iter, solver, class_weight
# MAGIC - **Métricas**: accuracy, precision, recall, f1_score, auc_roc
# MAGIC - **Artefatos**: modelo serializado, confusion matrix, feature importances

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Preparação dos Dados

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
        # Target
        F.when(F.col("o.status_entrega") == "atrasado", 1).otherwise(0).alias("label"),

        # Features temporais (disponiveis apos aprovacao)
        F.col("o.tempo_aprovacao_seg"),
        F.col("o.tempo_postagem_seg"),

        # Features do pedido
        F.col("o.total_pago"),
        F.col("o.max_parcelas"),

        # Features do pagamento (one-hot encoding manual)
        F.when(F.col("o.grupo_pagamento_principal") == "cartao", 1).otherwise(0).alias("pag_cartao"),
        F.when(F.col("o.grupo_pagamento_principal") == "boleto", 1).otherwise(0).alias("pag_boleto"),

        # Features geograficas do cliente
        F.col("o.customer_state"),

        # Features dos itens
        F.col("i.total_itens_valor"),
        F.col("i.total_frete"),
        F.col("i.peso_medio_kg"),
        F.col("i.volume_medio_cm3"),
        F.col("i.qtd_itens"),

        # Features geograficas do vendedor
        F.col("i.seller_state"),
    )
)

print(f"Total de registros: {df.count()}")
print(f"Distribuicao do target:")
df.groupBy("label").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Feature Engineering

# COMMAND ----------

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline

df_clean = df.na.fill({
    "tempo_aprovacao_seg": 0,
    "tempo_postagem_seg": 0,
    "total_pago": 0,
    "max_parcelas": 1,
    "total_itens_valor": 0,
    "total_frete": 0,
    "peso_medio_kg": 0,
    "volume_medio_cm3": 0,
    "qtd_itens": 1,
})

customer_state_indexer = StringIndexer(
    inputCol="customer_state", outputCol="customer_state_idx", handleInvalid="keep"
)
seller_state_indexer = StringIndexer(
    inputCol="seller_state", outputCol="seller_state_idx", handleInvalid="keep"
)

numeric_features = [
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
    "customer_state_idx",
    "seller_state_idx",
]

assembler = VectorAssembler(inputCols=numeric_features, outputCol="features", handleInvalid="skip")

feature_pipeline = Pipeline(stages=[customer_state_indexer, seller_state_indexer, assembler])
pipeline_model = feature_pipeline.fit(df_clean)
df_features = pipeline_model.transform(df_clean)

print(f"Features: {numeric_features}")
print(f"Total features: {len(numeric_features)}")
df_features.select("label", "features").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Split Treino/Teste

# COMMAND ----------

train_df, test_df = df_features.randomSplit([0.8, 0.2], seed=42)

train_count = train_df.count()
test_count = test_df.count()
train_positive = train_df.filter(F.col("label") == 1).count()
test_positive = test_df.filter(F.col("label") == 1).count()

print(f"Treino: {train_count} registros ({train_positive} atrasados = {train_positive/train_count*100:.1f}%)")
print(f"Teste:  {test_count} registros ({test_positive} atrasados = {test_positive/test_count*100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Treinamento com MLflow Tracking

# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

experiment_name = "/Users/david.lopes@al.infnet.edu.br/pb-brasilmart-predicao-atraso"
mlflow.set_experiment(experiment_name)

print(f"MLflow Experiment: {experiment_name}")
print(f"MLflow Tracking URI: {mlflow.get_tracking_uri()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Run 1 — Regressão Logística (baseline)

# COMMAND ----------

with mlflow.start_run(run_name="logistic_regression_baseline") as run:
    # Hiperparametros
    reg_param = 0.01
    max_iter = 100
    elastic_net = 0.0

    mlflow.log_param("model_type", "LogisticRegression")
    mlflow.log_param("regParam", reg_param)
    mlflow.log_param("maxIter", max_iter)
    mlflow.log_param("elasticNetParam", elastic_net)
    mlflow.log_param("solver", "auto")
    mlflow.log_param("features", numeric_features)
    mlflow.log_param("num_features", len(numeric_features))
    mlflow.log_param("train_size", train_count)
    mlflow.log_param("test_size", test_count)
    mlflow.log_param("target", "status_entrega (atrasado=1, no_prazo=0)")

    # Dataset info
    mlflow.log_param("dataset", "pb_brasilmart.silver.orders_enriched + items_enriched")
    mlflow.log_param("split_ratio", "80/20")
    mlflow.log_param("seed", 42)

    # Treinar
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        regParam=reg_param,
        maxIter=max_iter,
        elasticNetParam=elastic_net,
    )
    lr_model = lr.fit(train_df)

    # Predicao
    predictions = lr_model.transform(test_df)

    # Metricas
    binary_eval = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
    multi_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")

    auc_roc = binary_eval.evaluate(predictions, {binary_eval.metricName: "areaUnderROC"})
    auc_pr = binary_eval.evaluate(predictions, {binary_eval.metricName: "areaUnderPR"})
    accuracy = multi_eval.evaluate(predictions, {multi_eval.metricName: "accuracy"})
    precision = multi_eval.evaluate(predictions, {multi_eval.metricName: "weightedPrecision"})
    recall = multi_eval.evaluate(predictions, {multi_eval.metricName: "weightedRecall"})
    f1 = multi_eval.evaluate(predictions, {multi_eval.metricName: "f1"})

    mlflow.log_metric("auc_roc", auc_roc)
    mlflow.log_metric("auc_pr", auc_pr)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)
    mlflow.log_metric("f1_score", f1)

    # Feature importances (coeficientes)
    coefficients = lr_model.coefficients.toArray()
    feature_importance = sorted(
        zip(numeric_features, coefficients),
        key=lambda x: abs(x[1]),
        reverse=True
    )

    importance_text = "Feature Importances (|coeficiente|):\n"
    importance_text += "-" * 50 + "\n"
    for feat, coef in feature_importance:
        importance_text += f"  {feat:30s} {coef:+.6f}\n"
        mlflow.log_metric(f"coef_{feat}", coef)

    mlflow.log_text(importance_text, "feature_importances.txt")

    # Confusion matrix
    cm = (
        predictions.groupBy("label", "prediction")
        .count()
        .orderBy("label", "prediction")
        .collect()
    )
    cm_text = "Confusion Matrix:\n"
    cm_text += f"{'':>20} Pred=0   Pred=1\n"
    cm_dict = {(r["label"], int(r["prediction"])): r["count"] for r in cm}
    cm_text += f"  Real=0 (no_prazo)  {cm_dict.get((0,0), 0):>6}   {cm_dict.get((0,1), 0):>6}\n"
    cm_text += f"  Real=1 (atrasado)  {cm_dict.get((1,0), 0):>6}   {cm_dict.get((1,1), 0):>6}\n"
    mlflow.log_text(cm_text, "confusion_matrix.txt")

    # Log do modelo
    mlflow.spark.log_model(lr_model, "modelo_predicao_atraso")

    # Tags
    mlflow.set_tag("projeto", "pb-brasilmart")
    mlflow.set_tag("tp", "tp4")
    mlflow.set_tag("problema_negocio", "predicao_atraso_entrega")
    mlflow.set_tag("requisito_tp1", "Questao 3 - Diretor de Operacoes")

    run_id_baseline = run.info.run_id

    print(f"Run ID: {run_id_baseline}")
    print(f"\nMetricas:")
    print(f"  AUC-ROC:   {auc_roc:.4f}")
    print(f"  AUC-PR:    {auc_pr:.4f}")
    print(f"  Accuracy:  {accuracy:.4f}")
    print(f"  Precision: {precision:.4f}")
    print(f"  Recall:    {recall:.4f}")
    print(f"  F1-Score:  {f1:.4f}")
    print(f"\n{importance_text}")
    print(cm_text)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Run 2 — Regressão Logística (regularização L1 + class_weight)

# COMMAND ----------

with mlflow.start_run(run_name="logistic_regression_l1_balanced") as run:
    reg_param = 0.1
    max_iter = 200
    elastic_net = 1.0

    positive_ratio = train_positive / train_count
    weight_ratio = (1 - positive_ratio) / positive_ratio

    train_weighted = train_df.withColumn(
        "weight",
        F.when(F.col("label") == 1, weight_ratio).otherwise(1.0)
    )

    mlflow.log_param("model_type", "LogisticRegression")
    mlflow.log_param("regParam", reg_param)
    mlflow.log_param("maxIter", max_iter)
    mlflow.log_param("elasticNetParam", elastic_net)
    mlflow.log_param("regularization", "L1 (Lasso)")
    mlflow.log_param("class_weight", f"balanced (weight_ratio={weight_ratio:.2f})")
    mlflow.log_param("features", numeric_features)
    mlflow.log_param("num_features", len(numeric_features))
    mlflow.log_param("train_size", train_count)
    mlflow.log_param("test_size", test_count)
    mlflow.log_param("target", "status_entrega (atrasado=1, no_prazo=0)")
    mlflow.log_param("dataset", "pb_brasilmart.silver.orders_enriched + items_enriched")
    mlflow.log_param("split_ratio", "80/20")
    mlflow.log_param("seed", 42)

    lr2 = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        weightCol="weight",
        regParam=reg_param,
        maxIter=max_iter,
        elasticNetParam=elastic_net,
    )
    lr_model2 = lr2.fit(train_weighted)

    predictions2 = lr_model2.transform(test_df)

    auc_roc2 = binary_eval.evaluate(predictions2, {binary_eval.metricName: "areaUnderROC"})
    auc_pr2 = binary_eval.evaluate(predictions2, {binary_eval.metricName: "areaUnderPR"})
    accuracy2 = multi_eval.evaluate(predictions2, {multi_eval.metricName: "accuracy"})
    precision2 = multi_eval.evaluate(predictions2, {multi_eval.metricName: "weightedPrecision"})
    recall2 = multi_eval.evaluate(predictions2, {multi_eval.metricName: "weightedRecall"})
    f12 = multi_eval.evaluate(predictions2, {multi_eval.metricName: "f1"})

    mlflow.log_metric("auc_roc", auc_roc2)
    mlflow.log_metric("auc_pr", auc_pr2)
    mlflow.log_metric("accuracy", accuracy2)
    mlflow.log_metric("precision", precision2)
    mlflow.log_metric("recall", recall2)
    mlflow.log_metric("f1_score", f12)

    coefficients2 = lr_model2.coefficients.toArray()
    feature_importance2 = sorted(
        zip(numeric_features, coefficients2),
        key=lambda x: abs(x[1]),
        reverse=True
    )

    importance_text2 = "Feature Importances L1 (|coeficiente|):\n"
    importance_text2 += "-" * 50 + "\n"
    for feat, coef in feature_importance2:
        importance_text2 += f"  {feat:30s} {coef:+.6f}\n"
        mlflow.log_metric(f"coef_{feat}", coef)
    mlflow.log_text(importance_text2, "feature_importances.txt")

    cm2 = (
        predictions2.groupBy("label", "prediction")
        .count()
        .orderBy("label", "prediction")
        .collect()
    )
    cm_text2 = "Confusion Matrix (L1 balanced):\n"
    cm_text2 += f"{'':>20} Pred=0   Pred=1\n"
    cm_dict2 = {(r["label"], int(r["prediction"])): r["count"] for r in cm2}
    cm_text2 += f"  Real=0 (no_prazo)  {cm_dict2.get((0,0), 0):>6}   {cm_dict2.get((0,1), 0):>6}\n"
    cm_text2 += f"  Real=1 (atrasado)  {cm_dict2.get((1,0), 0):>6}   {cm_dict2.get((1,1), 0):>6}\n"
    mlflow.log_text(cm_text2, "confusion_matrix.txt")

    mlflow.spark.log_model(lr_model2, "modelo_predicao_atraso_l1")

    mlflow.set_tag("projeto", "pb-brasilmart")
    mlflow.set_tag("tp", "tp4")
    mlflow.set_tag("problema_negocio", "predicao_atraso_entrega")
    mlflow.set_tag("requisito_tp1", "Questao 3 - Diretor de Operacoes")

    run_id_l1 = run.info.run_id

    print(f"Run ID: {run_id_l1}")
    print(f"\nMetricas (L1 Balanced):")
    print(f"  AUC-ROC:   {auc_roc2:.4f}")
    print(f"  AUC-PR:    {auc_pr2:.4f}")
    print(f"  Accuracy:  {accuracy2:.4f}")
    print(f"  Precision: {precision2:.4f}")
    print(f"  Recall:    {recall2:.4f}")
    print(f"  F1-Score:  {f12:.4f}")
    print(f"\n{importance_text2}")
    print(cm_text2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Run 3 — Regressão Logística (regularização L2 + ElasticNet)

# COMMAND ----------

with mlflow.start_run(run_name="logistic_regression_elasticnet") as run:
    reg_param = 0.05
    max_iter = 150
    elastic_net = 0.5

    mlflow.log_param("model_type", "LogisticRegression")
    mlflow.log_param("regParam", reg_param)
    mlflow.log_param("maxIter", max_iter)
    mlflow.log_param("elasticNetParam", elastic_net)
    mlflow.log_param("regularization", "ElasticNet (L1+L2, alpha=0.5)")
    mlflow.log_param("class_weight", f"balanced (weight_ratio={weight_ratio:.2f})")
    mlflow.log_param("features", numeric_features)
    mlflow.log_param("num_features", len(numeric_features))
    mlflow.log_param("train_size", train_count)
    mlflow.log_param("test_size", test_count)
    mlflow.log_param("target", "status_entrega (atrasado=1, no_prazo=0)")
    mlflow.log_param("dataset", "pb_brasilmart.silver.orders_enriched + items_enriched")

    lr3 = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        weightCol="weight",
        regParam=reg_param,
        maxIter=max_iter,
        elasticNetParam=elastic_net,
    )
    lr_model3 = lr3.fit(train_weighted)

    predictions3 = lr_model3.transform(test_df)

    auc_roc3 = binary_eval.evaluate(predictions3, {binary_eval.metricName: "areaUnderROC"})
    auc_pr3 = binary_eval.evaluate(predictions3, {binary_eval.metricName: "areaUnderPR"})
    accuracy3 = multi_eval.evaluate(predictions3, {multi_eval.metricName: "accuracy"})
    precision3 = multi_eval.evaluate(predictions3, {multi_eval.metricName: "weightedPrecision"})
    recall3 = multi_eval.evaluate(predictions3, {multi_eval.metricName: "weightedRecall"})
    f13 = multi_eval.evaluate(predictions3, {multi_eval.metricName: "f1"})

    mlflow.log_metric("auc_roc", auc_roc3)
    mlflow.log_metric("auc_pr", auc_pr3)
    mlflow.log_metric("accuracy", accuracy3)
    mlflow.log_metric("precision", precision3)
    mlflow.log_metric("recall", recall3)
    mlflow.log_metric("f1_score", f13)

    coefficients3 = lr_model3.coefficients.toArray()
    for feat, coef in zip(numeric_features, coefficients3):
        mlflow.log_metric(f"coef_{feat}", coef)

    mlflow.spark.log_model(lr_model3, "modelo_predicao_atraso_elasticnet")

    mlflow.set_tag("projeto", "pb-brasilmart")
    mlflow.set_tag("tp", "tp4")

    run_id_en = run.info.run_id

    print(f"Run ID: {run_id_en}")
    print(f"\nMetricas (ElasticNet Balanced):")
    print(f"  AUC-ROC:   {auc_roc3:.4f}")
    print(f"  AUC-PR:    {auc_pr3:.4f}")
    print(f"  Accuracy:  {accuracy3:.4f}")
    print(f"  Precision: {precision3:.4f}")
    print(f"  Recall:    {recall3:.4f}")
    print(f"  F1-Score:  {f13:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Comparação dos Runs no MLflow

# COMMAND ----------

print("=" * 80)
print("COMPARACAO DOS MODELOS — MLflow Experiment: pb-brasilmart-predicao-atraso")
print("=" * 80)
print(f"{'Metrica':<20} {'Baseline (L2)':>15} {'L1 Balanced':>15} {'ElasticNet':>15}")
print("-" * 80)
print(f"{'AUC-ROC':<20} {auc_roc:>15.4f} {auc_roc2:>15.4f} {auc_roc3:>15.4f}")
print(f"{'AUC-PR':<20} {auc_pr:>15.4f} {auc_pr2:>15.4f} {auc_pr3:>15.4f}")
print(f"{'Accuracy':<20} {accuracy:>15.4f} {accuracy2:>15.4f} {accuracy3:>15.4f}")
print(f"{'Precision':<20} {precision:>15.4f} {precision2:>15.4f} {precision3:>15.4f}")
print(f"{'Recall':<20} {recall:>15.4f} {recall2:>15.4f} {recall3:>15.4f}")
print(f"{'F1-Score':<20} {f1:>15.4f} {f12:>15.4f} {f13:>15.4f}")
print("-" * 80)

best_f1 = max([(f1, "Baseline"), (f12, "L1 Balanced"), (f13, "ElasticNet")])
best_auc = max([(auc_roc, "Baseline"), (auc_roc2, "L1 Balanced"), (auc_roc3, "ElasticNet")])
print(f"\nMelhor F1-Score:  {best_f1[1]} ({best_f1[0]:.4f})")
print(f"Melhor AUC-ROC:   {best_auc[1]} ({best_auc[0]:.4f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Registrar Melhor Modelo no MLflow Model Registry

# COMMAND ----------

model_name = "pb-brasilmart-predicao-atraso"

best_run_id = run_id_baseline
if f12 > f1 and f12 >= f13:
    best_run_id = run_id_l1
elif f13 > f1 and f13 > f12:
    best_run_id = run_id_en

best_model_uri = f"runs:/{best_run_id}/modelo_predicao_atraso"
if best_run_id == run_id_l1:
    best_model_uri = f"runs:/{best_run_id}/modelo_predicao_atraso_l1"
elif best_run_id == run_id_en:
    best_model_uri = f"runs:/{best_run_id}/modelo_predicao_atraso_elasticnet"

result = mlflow.register_model(
    model_uri=best_model_uri,
    name=model_name
)

print(f"Modelo registrado no MLflow Model Registry:")
print(f"  Nome:    {model_name}")
print(f"  Versao:  {result.version}")
print(f"  Run ID:  {best_run_id}")
print(f"  Status:  {result.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Transicionar para Staging

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()

client.set_registered_model_tag(model_name, "projeto", "pb-brasilmart")
client.set_registered_model_tag(model_name, "problema", "predicao_atraso_entrega")
client.set_registered_model_tag(model_name, "tp", "tp4")

client.set_model_version_tag(
    model_name, result.version, "validation_status", "pendente"
)

client.update_model_version(
    name=model_name,
    version=result.version,
    description=(
        "Regressao Logistica para predicao de atraso na entrega. "
        "Features: tempo_aprovacao, peso, frete, valor, regiao. "
        "Target: status_entrega (atrasado=1, no_prazo=0). "
        "Requisito TP1 Questao 3: Diretor de Operacoes."
    )
)

print(f"Modelo {model_name} v{result.version} registrado com descricao e tags.")
print(f"Proximo passo: validar metricas e transicionar para Production.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Evidências — MLflow UI

# COMMAND ----------

print("=" * 70)
print("EVIDENCIAS MLflow — Como verificar no Databricks")
print("=" * 70)
print("""
1. EXPERIMENTS (Tracking):
   Sidebar → Experiments → pb-brasilmart-predicao-atraso
   - 3 runs visiveis com metricas, params e artefatos
   - Comparar runs: selecionar todos → Compare

2. MODEL REGISTRY:
   Sidebar → Models → pb-brasilmart-predicao-atraso
   - Versao registrada com descricao
   - Tags: projeto, problema, tp
   - Lineage: link para o run de origem

3. ARTEFATOS (por run):
   - modelo_predicao_atraso/ → modelo serializado (SparkML)
   - feature_importances.txt → coeficientes ordenados
   - confusion_matrix.txt → matriz de confusao

4. HIPERPARAMETROS LOGADOS:
   - model_type, regParam, maxIter, elasticNetParam
   - solver, class_weight, features, num_features
   - train_size, test_size, split_ratio, seed
   - dataset, target

5. METRICAS LOGADAS:
   - auc_roc, auc_pr, accuracy, precision, recall, f1_score
   - coef_* (coeficientes por feature)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo TP4 — Atividade 3: MLOps Base
# MAGIC
# MAGIC ### Modelo
# MAGIC | Item | Detalhe |
# MAGIC |------|---------|
# MAGIC | Problema | Classificação binária: entrega atrasou? (atrasado vs no_prazo) |
# MAGIC | Algoritmo | Regressão Logística (Spark MLlib) |
# MAGIC | Features | 13 (temporais, financeiras, geográficas, produto) |
# MAGIC | Target | `status_entrega` derivado de `delta_entrega_dias` |
# MAGIC | Dados | `silver.orders_enriched` JOIN `silver.items_enriched` |
# MAGIC | Requisito TP1 | Questão 3 — Diretor de Operações: "modelo preditivo de atraso" |
# MAGIC
# MAGIC ### MLflow Tracking
# MAGIC | Item | Registrado |
# MAGIC |------|------------|
# MAGIC | Experiment | `pb-brasilmart-predicao-atraso` |
# MAGIC | Runs | 3 (Baseline L2, L1 Balanced, ElasticNet) |
# MAGIC | Hiperparâmetros | regParam, maxIter, elasticNetParam, solver, class_weight |
# MAGIC | Métricas | AUC-ROC, AUC-PR, accuracy, precision, recall, F1-score |
# MAGIC | Artefatos | Modelo SparkML, feature_importances.txt, confusion_matrix.txt |
# MAGIC | Model Registry | `pb-brasilmart-predicao-atraso` (versão registrada) |
# MAGIC
# MAGIC ### Features Utilizadas
# MAGIC | Feature | Origem | Tipo |
# MAGIC |---------|--------|------|
# MAGIC | `tempo_aprovacao_seg` | Silver orders | Temporal |
# MAGIC | `tempo_postagem_seg` | Silver orders | Temporal |
# MAGIC | `total_pago` | Silver orders_enriched | Financeira |
# MAGIC | `max_parcelas` | Silver orders_enriched | Financeira |
# MAGIC | `pag_cartao`, `pag_boleto` | Silver orders_enriched | Categórica (OHE) |
# MAGIC | `total_itens_valor` | Silver items | Financeira |
# MAGIC | `total_frete` | Silver items | Logística |
# MAGIC | `peso_medio_kg` | Silver items_enriched | Produto |
# MAGIC | `volume_medio_cm3` | Silver items_enriched | Produto |
# MAGIC | `qtd_itens` | Silver items | Pedido |
# MAGIC | `customer_state_idx` | Silver orders_enriched | Geográfica |
# MAGIC | `seller_state_idx` | Silver items_enriched | Geográfica |
