# Databricks notebook source
# MAGIC %md
# MAGIC # TP3 — 2.3 Alerta de Falha (Simulado)
# MAGIC
# MAGIC **Objetivo:** Notebook executado pelo Databricks Workflow quando o pipeline DLT
# MAGIC **falha**. Simula o envio de um alerta (e-mail/Slack) com detalhes do erro.
# MAGIC
# MAGIC Faz parte da **ramificação condicional** do workflow:
# MAGIC ```
# MAGIC Executar_DLT
# MAGIC   ├── [SUCESSO] → Executar_Notebook_Validacao → Otimizar_Tabela_Gold
# MAGIC   └── [FALHA]   → **Enviar_Alerta_Falha** (este notebook)
# MAGIC ```

# COMMAND ----------

from datetime import datetime
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Coletar Informações do Erro

# COMMAND ----------

timestamp_falha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
pipeline_name = "pb-brasilmart-silver"
catalog = "pb_brasilmart"
schema = "silver"

alerta = {
    "tipo": "ALERTA_PIPELINE_DLT",
    "severidade": "CRITICA",
    "timestamp": timestamp_falha,
    "pipeline": pipeline_name,
    "catalog": catalog,
    "schema": schema,
    "mensagem": f"O pipeline DLT '{pipeline_name}' falhou em {timestamp_falha}. Ação manual necessária.",
    "acao_recomendada": "Verificar logs do pipeline DLT no Databricks UI → Workflows → Delta Live Tables",
    "destinatarios": ["david.lopes@al.infnet.edu.br"],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Simular Envio de Alerta

# COMMAND ----------

print("=" * 70)
print("  ALERTA — FALHA NO PIPELINE DLT")
print("=" * 70)
print(f"  Timestamp:  {alerta['timestamp']}")
print(f"  Pipeline:   {alerta['pipeline']}")
print(f"  Severidade: {alerta['severidade']}")
print(f"  Catálogo:   {alerta['catalog']}.{alerta['schema']}")
print(f"")
print(f"  {alerta['mensagem']}")
print(f"")
print(f"  Ação: {alerta['acao_recomendada']}")
print(f"  Para: {', '.join(alerta['destinatarios'])}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Registrar Alerta em Tabela de Auditoria

# COMMAND ----------

df_alerta = spark.createDataFrame([{
    "tipo": alerta["tipo"],
    "severidade": alerta["severidade"],
    "timestamp_falha": alerta["timestamp"],
    "pipeline": alerta["pipeline"],
    "mensagem": alerta["mensagem"],
    "acao_recomendada": alerta["acao_recomendada"],
}])

(df_alerta.write
 .format("delta")
 .mode("append")
 .option("mergeSchema", "true")
 .saveAsTable("pb_brasilmart.silver.alertas_pipeline"))

print("Alerta registrado em pb_brasilmart.silver.alertas_pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Saída do Notebook
# MAGIC
# MAGIC Retorna JSON com detalhes do alerta para o Databricks Workflow.

# COMMAND ----------

dbutils.notebook.exit(json.dumps(alerta))
