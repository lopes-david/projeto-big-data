# Databricks notebook source
# MAGIC %md
# MAGIC # TP5 — Atividade 2.2: RAG (Geração Aumentada de Recuperação) — Descrição Conceitual
# MAGIC
# MAGIC ## Objetivo
# MAGIC Descrever o pipeline LLMOps necessário para implementar um sistema RAG que
# MAGIC consultaria a **base de reviews dos clientes** (armazenada no Unity Catalog)
# MAGIC para responder perguntas técnicas da equipe de Customer Success e Operações.
# MAGIC
# MAGIC ## Contexto de Negócio — Requisitos TP1
# MAGIC
# MAGIC | Stakeholder | Questão TP1 | Necessidade |
# MAGIC |-------------|-------------|-------------|
# MAGIC | **Customer Success** (Q11) | "65% das avaliações negativas mencionam atraso ou produto diferente. Não temos como confirmar em escala." | Consultar reviews em linguagem natural para análise de causa raiz |
# MAGIC | **Cientista de Dados** (Q7) | "Predição de churn — se houve problema logístico na última compra." | Buscar padrões textuais que indicam insatisfação pré-churn |
# MAGIC | **Diretor de Operações** (Q3) | "Isso nos custou R$ 2,3M em reembolsos no ano passado." | Identificar categorias/vendedores recorrentes em reclamações |
# MAGIC | **Diretor Marketplace** (Q4) | "Um vendedor com muitas vendas mas péssimas avaliações não é detectado." | Busca semântica em reviews por vendedor/produto |
# MAGIC
# MAGIC ## Base de Conhecimento (equivalente a "Manuais de Manutenção")
# MAGIC
# MAGIC No contexto da BrasilMart, os **"manuais"** que alimentam o RAG são:
# MAGIC
# MAGIC | Fonte | Tabela Unity Catalog | Registros | Descrição |
# MAGIC |-------|---------------------|-----------|-----------|
# MAGIC | Reviews (texto) | `pb_brasilmart.silver.reviews` | ~41 mil com comentário | Feedback textual do cliente: elogios, reclamações, relatos de problema |
# MAGIC | Reviews enriquecidas | JOIN com `orders_enriched` | idem | Reviews + contexto de entrega (atraso, delta_dias, região) |
# MAGIC | Catálogo de produtos | `pb_brasilmart.silver.products` | ~33 mil SKUs | Categoria, peso, dimensões, porte — contexto do produto avaliado |
# MAGIC | Score de vendedores | `pb_brasilmart.gold.dim_sellers_score` | ~3 mil sellers | Performance consolidada do vendedor referenciado na review |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Arquitetura do Pipeline RAG
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                        PIPELINE RAG — BrasilMart                            │
# MAGIC ├─────────────────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                             │
# MAGIC │  ┌──────────────────────────────────────────────────────────────┐            │
# MAGIC │  │  1. INGESTÃO E PREPARAÇÃO (Offline — batch diário)          │            │
# MAGIC │  │                                                              │            │
# MAGIC │  │  Unity Catalog                                               │            │
# MAGIC │  │  └─ pb_brasilmart.silver.reviews                            │            │
# MAGIC │  │  └─ pb_brasilmart.silver.orders_enriched                    │            │
# MAGIC │  │  └─ pb_brasilmart.silver.products                           │            │
# MAGIC │  │       │                                                      │            │
# MAGIC │  │       ▼                                                      │            │
# MAGIC │  │  JOIN + Enriquecimento                                       │            │
# MAGIC │  │  (review + contexto de entrega + produto + vendedor)         │            │
# MAGIC │  │       │                                                      │            │
# MAGIC │  │       ▼                                                      │            │
# MAGIC │  │  Chunking (1 documento = 1 review enriquecida)              │            │
# MAGIC │  │       │                                                      │            │
# MAGIC │  │       ▼                                                      │            │
# MAGIC │  │  Embedding Model                                             │            │
# MAGIC │  │  (Databricks Foundation Model API)                           │            │
# MAGIC │  │  databricks-bge-large-en / gte-large-en                      │            │
# MAGIC │  │       │                                                      │            │
# MAGIC │  │       ▼                                                      │            │
# MAGIC │  │  Vector Search Index                                         │            │
# MAGIC │  │  (Databricks Vector Search)                                  │            │
# MAGIC │  │  Índice: pb_brasilmart_reviews_vs                            │            │
# MAGIC │  └──────────────────────────────────────────────────────────────┘            │
# MAGIC │                                                                             │
# MAGIC │  ┌──────────────────────────────────────────────────────────────┐            │
# MAGIC │  │  2. RETRIEVAL + GENERATION (Online — por query)             │            │
# MAGIC │  │                                                              │            │
# MAGIC │  │  Pergunta do Usuário                                         │            │
# MAGIC │  │  "Quais os principais problemas reportados com              │            │
# MAGIC │  │   produtos da categoria 'eletronicos' no estado AM?"         │            │
# MAGIC │  │       │                                                      │            │
# MAGIC │  │       ▼                                                      │            │
# MAGIC │  │  Embedding da Query                                          │            │
# MAGIC │  │  (mesmo modelo de embedding)                                 │            │
# MAGIC │  │       │                                                      │            │
# MAGIC │  │       ▼                                                      │            │
# MAGIC │  │  Vector Search (Top-K = 10)                                  │            │
# MAGIC │  │  Busca por similaridade coseno                               │            │
# MAGIC │  │       │                                                      │            │
# MAGIC │  │       ▼                                                      │            │
# MAGIC │  │  Prompt Assembly                                             │            │
# MAGIC │  │  [System prompt + contexto recuperado + pergunta]            │            │
# MAGIC │  │       │                                                      │            │
# MAGIC │  │       ▼                                                      │            │
# MAGIC │  │  LLM (Foundation Model API)                                  │            │
# MAGIC │  │  DBRX / Llama 3 / Claude via External Model                 │            │
# MAGIC │  │       │                                                      │            │
# MAGIC │  │       ▼                                                      │            │
# MAGIC │  │  Resposta com citações (review_id, order_id)                │            │
# MAGIC │  └──────────────────────────────────────────────────────────────┘            │
# MAGIC │                                                                             │
# MAGIC └─────────────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pipeline LLMOps — Etapas Detalhadas
# MAGIC
# MAGIC ### 2.1 Ingestão e Armazenamento dos Documentos
# MAGIC
# MAGIC Os documentos-fonte são as reviews enriquecidas com contexto de entrega e produto.
# MAGIC Ficam armazenados em **Unity Catalog Volumes** para governança centralizada.
# MAGIC
# MAGIC ```
# MAGIC Volume: pb_brasilmart.gold.rag_documents
# MAGIC Formato: Delta Table (versionada, auditável)
# MAGIC ```

# COMMAND ----------

# Código ilustrativo — preparação dos documentos para RAG

from pyspark.sql import functions as F

catalog = "pb_brasilmart"

# JOIN: review + contexto de entrega + produto + vendedor
df_docs = (
    spark.table(f"{catalog}.silver.reviews").alias("r")
    .join(
        spark.table(f"{catalog}.silver.orders_enriched").alias("o"),
        "order_id", "inner"
    )
    .join(
        spark.table(f"{catalog}.silver.items_enriched").alias("i"),
        "order_id", "inner"
    )
    .filter(F.col("r.review_message").isNotNull())
    .filter(F.length(F.trim(F.col("r.review_message"))) > 10)
    .select(
        F.col("r.review_id"),
        F.col("r.order_id"),
        F.col("r.review_score"),
        F.col("r.review_message"),
        F.col("r.review_sentiment"),
        F.col("o.customer_state"),
        F.col("o.status_entrega"),
        F.col("o.delta_entrega_dias"),
        F.col("i.product_category"),
        F.col("i.seller_state"),
    )
    .dropDuplicates(["review_id"])
)

# Construir documento textual enriquecido (1 chunk = 1 review)
df_chunks = df_docs.withColumn(
    "documento",
    F.concat_ws(
        "\n",
        F.concat(F.lit("Review (score "), F.col("review_score"), F.lit("/5): "), F.col("review_message")),
        F.concat(F.lit("Categoria: "), F.coalesce(F.col("product_category"), F.lit("N/A"))),
        F.concat(F.lit("Estado cliente: "), F.coalesce(F.col("customer_state"), F.lit("N/A"))),
        F.concat(F.lit("Estado vendedor: "), F.coalesce(F.col("seller_state"), F.lit("N/A"))),
        F.concat(F.lit("Entrega: "), F.coalesce(F.col("status_entrega"), F.lit("N/A")),
                 F.lit(" (delta: "), F.coalesce(F.col("delta_entrega_dias").cast("string"), F.lit("N/A")),
                 F.lit(" dias)")),
    )
)

print(f"Total de documentos (chunks): {df_chunks.count():,}")
df_chunks.select("review_id", "documento").show(3, truncate=100)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Chunking Strategy
# MAGIC
# MAGIC | Aspecto | Decisão | Justificativa |
# MAGIC |---------|---------|---------------|
# MAGIC | **Granularidade** | 1 chunk = 1 review enriquecida | Reviews são curtas (média ~50 palavras); não precisam ser divididas |
# MAGIC | **Conteúdo do chunk** | Texto da review + metadados (categoria, região, status entrega) | Metadados melhoram a relevância do retrieval (filtro semântico) |
# MAGIC | **Identificador** | `review_id` | Permite rastreabilidade até a review original no Unity Catalog |
# MAGIC | **Tamanho máximo** | ~300 tokens | Dentro do limite de embedding models (512 tokens) |
# MAGIC | **Overlap** | N/A | Chunks independentes (1 review = 1 unidade semântica) |
# MAGIC
# MAGIC **Por que não dividir?** Reviews de e-commerce são textos curtos e autocontidos.
# MAGIC Diferente de manuais técnicos longos, cada review é uma unidade semântica completa
# MAGIC — quebrá-la perderia contexto sem ganho de granularidade.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Embedding e Indexação Vetorial
# MAGIC
# MAGIC | Componente | Tecnologia Databricks | Detalhe |
# MAGIC |------------|----------------------|---------|
# MAGIC | **Embedding Model** | Foundation Model API | `databricks-bge-large-en` (768 dims) ou `databricks-gte-large-en` (1024 dims) |
# MAGIC | **Vector Store** | Databricks Vector Search | Managed index, auto-sync com Delta Table |
# MAGIC | **Índice** | Delta Sync Index | Sincroniza automaticamente quando a tabela-fonte é atualizada |
# MAGIC | **Métrica de similaridade** | Cosine Similarity | Padrão para embeddings normalizados |
# MAGIC | **Armazenamento** | Unity Catalog | Governança, permissões e linhagem integradas |

# COMMAND ----------

# Código ilustrativo — criação do Vector Search Index

# 1. Salvar documentos como Delta Table no Unity Catalog
# df_chunks.write.format("delta").mode("overwrite") \
#     .saveAsTable(f"{catalog}.gold.rag_reviews_documents")

# 2. Criar endpoint de Vector Search
# from databricks.vector_search.client import VectorSearchClient
#
# vsc = VectorSearchClient()
#
# # Criar endpoint (compute dedicado para servir embeddings)
# vsc.create_endpoint(
#     name="pb-brasilmart-vs-endpoint",
#     endpoint_type="STANDARD",
# )

# 3. Criar índice Delta Sync (auto-atualização)
# index = vsc.create_delta_sync_index(
#     endpoint_name="pb-brasilmart-vs-endpoint",
#     index_name=f"{catalog}.gold.rag_reviews_index",
#     source_table_name=f"{catalog}.gold.rag_reviews_documents",
#     pipeline_type="TRIGGERED",          # ou CONTINUOUS
#     primary_key="review_id",
#     embedding_source_column="documento", # coluna a ser vetorizada
#     embedding_model_endpoint_name="databricks-bge-large-en",
# )

print("Código ilustrativo — executar no Databricks com Vector Search habilitado")
print("Componentes:")
print("  1. Delta Table: pb_brasilmart.gold.rag_reviews_documents")
print("  2. VS Endpoint: pb-brasilmart-vs-endpoint")
print("  3. VS Index:    pb_brasilmart.gold.rag_reviews_index")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Retrieval — Busca Semântica
# MAGIC
# MAGIC Quando um usuário faz uma pergunta, o sistema:
# MAGIC 1. Gera o embedding da pergunta (mesmo modelo)
# MAGIC 2. Busca os Top-K documentos mais similares no índice vetorial
# MAGIC 3. Aplica filtros opcionais de metadados (região, categoria, score)

# COMMAND ----------

# Código ilustrativo — busca semântica

# from databricks.vector_search.client import VectorSearchClient
#
# vsc = VectorSearchClient()
# index = vsc.get_index(
#     endpoint_name="pb-brasilmart-vs-endpoint",
#     index_name=f"{catalog}.gold.rag_reviews_index",
# )
#
# # Busca semântica com filtro de metadados
# results = index.similarity_search(
#     query_text="problemas com atraso na entrega de eletrônicos no Amazonas",
#     columns=["review_id", "documento", "review_score",
#              "customer_state", "product_category", "status_entrega"],
#     num_results=10,
#     filters={"customer_state": "AM"},  # filtro opcional
# )

# Exemplo de resultado esperado:
print("Exemplo de retrieval — Top 3 resultados para:")
print('  Query: "problemas com atraso na entrega de eletrônicos no Amazonas"')
print()
print("  1. [score=0.89] review_id=abc123")
print('     "Produto demorou 45 dias para chegar em Manaus. Prazo era 15 dias."')
print("     Categoria: eletronicos | Estado: AM | Entrega: atrasado (delta: 30 dias)")
print()
print("  2. [score=0.85] review_id=def456")
print('     "Comprei um celular e veio com a tela trincada depois de 1 mês de espera"')
print("     Categoria: telefonia | Estado: AM | Entrega: atrasado (delta: 22 dias)")
print()
print("  3. [score=0.82] review_id=ghi789")
print('     "Nunca mais compro. Produto extraviado, tive que abrir reclamação."')
print("     Categoria: eletronicos | Estado: PA | Entrega: atrasado (delta: 40 dias)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Generation — Resposta com LLM
# MAGIC
# MAGIC O contexto recuperado é montado em um prompt estruturado e enviado ao LLM
# MAGIC para gerar uma resposta em linguagem natural com citações.

# COMMAND ----------

# Código ilustrativo — geração da resposta com Foundation Model API

SYSTEM_PROMPT = """Você é um assistente de análise de reviews da BrasilMart.
Responda perguntas sobre a experiência dos clientes usando APENAS as reviews
fornecidas como contexto. Cada review tem um review_id — cite-o ao referenciar.

Regras:
- Baseie sua resposta exclusivamente nas reviews fornecidas (não invente dados)
- Cite o review_id de cada review usada como evidência
- Se não houver reviews relevantes, diga "Não encontrei reviews sobre esse tema"
- Inclua dados quantitativos quando possível (contagens, proporções)
- Responda em português brasileiro
"""

# def gerar_resposta_rag(query: str, contexto_reviews: list[dict]) -> str:
#     """Monta prompt e chama LLM via Foundation Model API."""
#
#     contexto_formatado = "\n\n".join([
#         f"[review_id={r['review_id']}] (score={r['review_score']}/5, "
#         f"entrega={r['status_entrega']}):\n{r['documento']}"
#         for r in contexto_reviews
#     ])
#
#     prompt = f"""CONTEXTO (reviews recuperadas):
# {contexto_formatado}
#
# PERGUNTA: {query}
#
# Responda com base nas reviews acima, citando review_ids como evidência."""
#
#     # Chamada ao LLM via Foundation Model API
#     from databricks.sdk import WorkspaceClient
#     w = WorkspaceClient()
#
#     response = w.serving_endpoints.query(
#         name="databricks-meta-llama-3-1-70b-instruct",
#         messages=[
#             {"role": "system", "content": SYSTEM_PROMPT},
#             {"role": "user", "content": prompt},
#         ],
#         max_tokens=1024,
#         temperature=0.1,
#     )
#
#     return response.choices[0].message.content

print("System Prompt do RAG:")
print(SYSTEM_PROMPT)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exemplo de Interação RAG
# MAGIC
# MAGIC **Pergunta:** "Quais os principais problemas reportados com produtos da categoria
# MAGIC eletrônicos no estado do Amazonas?"
# MAGIC
# MAGIC **Resposta gerada (exemplo):**
# MAGIC > Com base nas reviews analisadas, os principais problemas na categoria eletrônicos
# MAGIC > para clientes do Amazonas são:
# MAGIC >
# MAGIC > 1. **Atraso na entrega** (6 de 10 reviews): Pedidos levam em média 30 dias além
# MAGIC >    do prazo estimado. [review_id=abc123, review_id=def456, review_id=jkl012, ...]
# MAGIC >
# MAGIC > 2. **Produto danificado no transporte** (2 de 10 reviews): Telas trincadas e
# MAGIC >    embalagens violadas após longo trajeto logístico. [review_id=def456, review_id=mno345]
# MAGIC >
# MAGIC > 3. **Extravio** (2 de 10 reviews): Pedidos que nunca chegaram, exigindo
# MAGIC >    reclamação formal. [review_id=ghi789, review_id=pqr678]
# MAGIC >
# MAGIC > **Correlação com dados de entrega:** 80% dessas reviews correspondem a pedidos
# MAGIC > com `status_entrega = atrasado` e `delta_entrega > 20 dias`, confirmando que
# MAGIC > o problema é predominantemente logístico para a região Norte.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pipeline LLMOps — Ciclo de Vida Completo
# MAGIC
# MAGIC ### 3.1 Visão Geral do Ciclo LLMOps
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                          CICLO LLMOps                                      │
# MAGIC │                                                                            │
# MAGIC │  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐             │
# MAGIC │  │  1. DATA  │───→│ 2. BUILD │───→│ 3. EVAL  │───→│ 4. DEPLOY│             │
# MAGIC │  │  PREP     │    │          │    │          │    │          │             │
# MAGIC │  └──────────┘    └──────────┘    └──────────┘    └────┬─────┘             │
# MAGIC │       │                                               │                    │
# MAGIC │       │          ┌──────────┐    ┌──────────┐         │                    │
# MAGIC │       └─────────←│ 6. RETRAIN│←──│5. MONITOR│←────────┘                    │
# MAGIC │                  └──────────┘    └──────────┘                              │
# MAGIC └────────────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### 3.2 Etapas Detalhadas
# MAGIC
# MAGIC | Etapa | Ação | Ferramenta Databricks | Frequência |
# MAGIC |-------|------|-----------------------|------------|
# MAGIC | **1. Data Prep** | JOIN reviews + entrega + produto → chunking → Delta Table | Spark, Unity Catalog, Volumes | Diário (batch) |
# MAGIC | **2. Build** | Embedding dos chunks → Vector Search Index | Foundation Model API, Vector Search | Triggered/Contínuo |
# MAGIC | **3. Eval** | Avaliar qualidade do retrieval + geração (RAGAS, human eval) | MLflow Evaluation, Mosaic AI Agent Evaluation | A cada re-indexação |
# MAGIC | **4. Deploy** | Expor RAG como endpoint REST (Model Serving + Agent) | Mosaic AI Agent Framework, Model Serving | CI/CD (Databricks Asset Bundle) |
# MAGIC | **5. Monitor** | Latência, relevância, feedback do usuário, drift | Lakehouse Monitoring, MLflow, Inference Tables | Contínuo |
# MAGIC | **6. Retrain** | Re-indexar com novos dados, ajustar prompt, trocar modelo | Databricks Workflows (job agendado) | Semanal ou por trigger |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Componentes Databricks Utilizados
# MAGIC
# MAGIC | Componente | Papel no RAG | Detalhes |
# MAGIC |-----------|-------------|---------|
# MAGIC | **Unity Catalog** | Governança dos documentos e índice vetorial | Permissões, linhagem, auditoria — quem acessou quais reviews |
# MAGIC | **Unity Catalog Volumes** | Armazenamento de documentos-fonte | Volume `rag_documents` no schema Gold |
# MAGIC | **Databricks Vector Search** | Armazenamento e busca vetorial | Delta Sync Index com auto-atualização |
# MAGIC | **Foundation Model API** | Embedding + LLM para geração | `databricks-bge-large-en` (embedding), `databricks-meta-llama-3-1-70b-instruct` (geração) |
# MAGIC | **Model Serving** | Endpoint REST para o RAG chain | Escala automática, autenticação integrada |
# MAGIC | **Mosaic AI Agent Framework** | Orquestração retrieval → prompt → LLM | `langchain` ou `pyfunc` como agent |
# MAGIC | **MLflow** | Tracking de experimentos RAG, registro de chains | `mlflow.langchain.log_model` ou `mlflow.pyfunc.log_model` |
# MAGIC | **Mosaic AI Agent Evaluation** | Avaliação automatizada de qualidade | Métricas: faithfulness, relevance, groundedness |
# MAGIC | **Inference Tables** | Log de requests/responses para análise | Auditoria, feedback loop, detecção de drift |
# MAGIC | **Lakehouse Monitoring** | Monitoramento contínuo de qualidade | Alertas de degradação de relevância |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Avaliação de Qualidade (RAG Evaluation)
# MAGIC
# MAGIC | Métrica | O que mede | Ferramenta | Threshold |
# MAGIC |---------|-----------|------------|-----------|
# MAGIC | **Faithfulness** | Resposta é fiel ao contexto recuperado? (sem alucinação) | Mosaic AI Agent Evaluation | ≥ 0.85 |
# MAGIC | **Answer Relevance** | Resposta é relevante para a pergunta? | Mosaic AI Agent Evaluation | ≥ 0.80 |
# MAGIC | **Context Relevance** | Documentos recuperados são relevantes para a pergunta? | Mosaic AI Agent Evaluation | ≥ 0.75 |
# MAGIC | **Groundedness** | Cada afirmação da resposta tem suporte no contexto? | Mosaic AI Agent Evaluation | ≥ 0.80 |
# MAGIC | **Latência (P95)** | Tempo de resposta end-to-end | Inference Tables + Monitoring | ≤ 5s |
# MAGIC | **Retrieval Precision@K** | Proporção de docs recuperados que são relevantes | MLflow Custom Metrics | ≥ 0.70 |

# COMMAND ----------

# Código ilustrativo — avaliação com Mosaic AI Agent Evaluation

# import mlflow
#
# # Dataset de avaliação (perguntas + respostas esperadas)
# eval_dataset = [
#     {
#         "question": "Quais os problemas mais comuns em entregas para o Amazonas?",
#         "expected_answer": "Atraso na entrega e extravio são os problemas mais relatados",
#         "expected_sources": ["review_id_1", "review_id_2"],
#     },
#     {
#         "question": "Vendedores de SP entregam no prazo?",
#         "expected_answer": "Maioria sim, mas há outliers com alto cancelamento",
#         "expected_sources": ["review_id_3"],
#     },
# ]
#
# # Avaliar o chain RAG
# results = mlflow.evaluate(
#     model=rag_chain_uri,
#     data=eval_dataset,
#     model_type="databricks-agent",
# )
#
# print(f"Faithfulness:       {results.metrics['faithfulness/mean']:.3f}")
# print(f"Answer Relevance:   {results.metrics['answer_relevance/mean']:.3f}")
# print(f"Context Relevance:  {results.metrics['context_relevance/mean']:.3f}")
# print(f"Groundedness:       {results.metrics['groundedness/mean']:.3f}")

print("Avaliação seria executada com Mosaic AI Agent Evaluation")
print("Métricas-chave: faithfulness, answer_relevance, groundedness")
print("Threshold mínimo para deploy: faithfulness ≥ 0.85")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fluxo End-to-End — Perguntas Técnicas dos Stakeholders
# MAGIC
# MAGIC ### Exemplos de uso por stakeholder (TP1):
# MAGIC
# MAGIC | Stakeholder | Pergunta ao RAG | Reviews recuperadas | Ação derivada |
# MAGIC |-------------|----------------|--------------------|----|
# MAGIC | **Customer Success (Q11)** | "Quais as causas de reviews 1-estrela este mês?" | Reviews negativas recentes com contexto de entrega | Priorizar vendedores/regiões com mais reclamações |
# MAGIC | **Operações (Q3)** | "Quantas reclamações mencionam atraso na região Norte?" | Reviews com `status_entrega=atrasado` e `customer_state IN (AM,PA,AC,...)` | Renegociar SLA com transportadoras regionais |
# MAGIC | **Marketplace (Q4)** | "Qual o perfil de reclamações do vendedor X?" | Reviews do seller filtradas por seller_id | Notificar vendedor, aplicar penalidade no score |
# MAGIC | **Data Scientist (Q7)** | "Clientes que reclamaram de entrega voltam a comprar?" | Reviews negativas cruzadas com RFM (frequency, recency) | Definir features de churn preditivo |
# MAGIC | **Marketing (Q2)** | "O que clientes Champions elogiam nas reviews?" | Reviews score 5 de clientes com RFM=Champions | Usar insights para copy de campanhas de retenção |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Governança e Segurança (Unity Catalog)
# MAGIC
# MAGIC | Aspecto | Implementação |
# MAGIC |---------|--------------|
# MAGIC | **Acesso aos documentos** | Permissões Unity Catalog por schema/tabela — equipe de CS vê reviews, Financeiro não |
# MAGIC | **PII nos chunks** | Reviews podem conter nomes/CEPs — mascaramento via Dynamic Views ou Column Masks |
# MAGIC | **Auditoria** | Inference Tables logam cada query + resposta + documentos recuperados |
# MAGIC | **Linhagem** | Unity Catalog rastreia: `silver.reviews` → `gold.rag_reviews_documents` → `rag_reviews_index` |
# MAGIC | **Versionamento** | Delta Table versionada — possível restaurar índice para ponto no tempo |
# MAGIC | **Rate limiting** | Model Serving endpoint com limites por usuário/grupo |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Evidências

# COMMAND ----------

print("=" * 70)
print("EVIDÊNCIAS — TP5 Atividade 2.2: RAG Conceitual")
print("=" * 70)
print("""
1. PIPELINE RAG DESCRITO:
   Ingestão → Chunking → Embedding → Vector Search → Retrieval → LLM → Resposta

2. BASE DE CONHECIMENTO:
   Documentos: reviews enriquecidas (review + entrega + produto + vendedor)
   Equivalente a "manuais de manutenção" no contexto e-commerce
   Armazenamento: Unity Catalog Volumes + Delta Table

3. COMPONENTES DATABRICKS:
   - Unity Catalog (governança)
   - Databricks Vector Search (índice vetorial)
   - Foundation Model API (embedding + LLM)
   - Mosaic AI Agent Framework (orquestração)
   - Model Serving (endpoint REST)
   - Inference Tables (monitoramento)
   - MLflow (tracking + avaliação)

4. LLMOps CICLO COMPLETO:
   Data Prep → Build → Eval → Deploy → Monitor → Retrain

5. REQUISITOS TP1 ATENDIDOS:
   Q11 (Customer Success): análise de causa raiz de reviews
   Q7 (Cientista de Dados): features textuais para churn
   Q3 (Operações): padrões de atraso por região
   Q4 (Marketplace): perfil de reclamações por vendedor
   Q2 (Marketing): insights de clientes Champions

6. AVALIAÇÃO DE QUALIDADE:
   Métricas: faithfulness, answer_relevance, groundedness
   Ferramenta: Mosaic AI Agent Evaluation
   Threshold mínimo: faithfulness ≥ 0.85
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo TP5 — Atividade 2.2: RAG Conceitual
# MAGIC
# MAGIC ### Pipeline RAG
# MAGIC ```
# MAGIC ┌────────────────┐   ┌─────────────┐   ┌───────────────┐   ┌─────────────┐
# MAGIC │ Unity Catalog  │   │  Embedding   │   │ Vector Search │   │  LLM        │
# MAGIC │ silver.reviews │──→│  Foundation  │──→│  Delta Sync   │──→│  Foundation  │
# MAGIC │ + orders_enr.  │   │  Model API   │   │  Index        │   │  Model API  │
# MAGIC │ + products     │   │  (bge-large) │   │  (cosine sim) │   │  (Llama 3)  │
# MAGIC └────────────────┘   └─────────────┘   └───────────────┘   └──────┬──────┘
# MAGIC                                                                    │
# MAGIC                                                                    ▼
# MAGIC                                                           ┌──────────────┐
# MAGIC                                                           │  Resposta    │
# MAGIC                                                           │  + citações  │
# MAGIC                                                           │  (review_id) │
# MAGIC                                                           └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### LLMOps
# MAGIC | Etapa | Ferramenta | Frequência |
# MAGIC |-------|-----------|------------|
# MAGIC | Data Prep | Spark + Unity Catalog | Diário |
# MAGIC | Build | Vector Search + Foundation Model API | Triggered |
# MAGIC | Eval | Mosaic AI Agent Evaluation | A cada build |
# MAGIC | Deploy | Model Serving + Agent Framework | CI/CD |
# MAGIC | Monitor | Inference Tables + Lakehouse Monitoring | Contínuo |
# MAGIC | Retrain | Databricks Workflows | Semanal |
