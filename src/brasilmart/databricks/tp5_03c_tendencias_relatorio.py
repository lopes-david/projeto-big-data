# Databricks notebook source
# MAGIC %md
# MAGIC # TP5 — Atividade 3.3: Tendências — Data Fabric e Lambda vs Kappa
# MAGIC
# MAGIC ## Contexto
# MAGIC Análise da aplicabilidade de uma arquitetura **Data Fabric** na BrasilMart
# MAGIC e avaliação de qual abordagem — **Lambda** ou **Kappa** — seria mais adequada
# MAGIC para os dados de telemetria (eventos de pedidos, entregas e pagamentos).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Aplicabilidade de Data Fabric na BrasilMart
# MAGIC
# MAGIC A BrasilMart opera com uma arquitetura híbrida que integra múltiplos serviços
# MAGIC heterogêneos — AWS S3, Glue, Lake Formation e Redshift no lado AWS, e Unity
# MAGIC Catalog, Delta Live Tables, MLflow e Model Serving no lado Databricks — além
# MAGIC de um projeto dbt que orquestra transformações no Data Warehouse. Essa
# MAGIC fragmentação é exatamente o cenário onde uma arquitetura **Data Fabric** entrega
# MAGIC maior valor. O Data Fabric atuaria como uma camada de metadados inteligente
# MAGIC que **unificaria a governança, a descoberta e o acesso aos dados** dispersos
# MAGIC entre os dois ecossistemas, sem exigir a migração física para uma plataforma
# MAGIC única. Na prática, o Unity Catalog já exerce parte desse papel ao centralizar
# MAGIC catálogo, permissões (column-level e row-level security), linhagem e auditoria
# MAGIC para os dados no Databricks, enquanto o Lake Formation faz o mesmo para os
# MAGIC dados na AWS. Um Data Fabric completo estenderia essa governança com uma
# MAGIC **camada semântica unificada** — um knowledge graph que mapearia
# MAGIC automaticamente as relações entre `dim_clientes_rfm` no Redshift,
# MAGIC `silver.orders_enriched` no Unity Catalog e o modelo
# MAGIC `pb-brasilmart-predicao-atraso` no MLflow Registry, permitindo que um analista
# MAGIC descobrisse, por exemplo, que a feature `delta_entrega_dias` percorre um
# MAGIC caminho de linhagem que vai do CSV bruto no S3 até a probabilidade de atraso
# MAGIC servida via REST endpoint, passando por Bronze, Silver (DLT), Gold (dbt) e
# MAGIC o modelo sklearn — tudo sem precisar conhecer os 5 sistemas intermediários.
# MAGIC Além disso, o pilar de **automação por ML** do Data Fabric — que recomenda
# MAGIC políticas de acesso, detecta PII automaticamente e sugere otimizações de
# MAGIC schema — resolveria diretamente problemas que hoje são manuais no projeto:
# MAGIC a descoberta de PII (TP4) foi feita por scan manual de 13 colunas em 5
# MAGIC tabelas, e a classificação de reviews (TP5) exigiu GenAI explícito; um Data
# MAGIC Fabric maduro faria ambos de forma contínua e autônoma. A principal barreira
# MAGIC para a adoção é a **maturidade organizacional**: a BrasilMart precisaria de
# MAGIC um catálogo de metadados federado (como Databricks Unity Catalog com
# MAGIC conectores externos ou uma solução como Apache Atlas / Atlan) que enxergasse
# MAGIC simultaneamente os dados no Redshift, no S3 e no Databricks, algo que hoje
# MAGIC exigiria integração customizada entre Lake Formation e Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Lambda vs Kappa para Dados de Telemetria
# MAGIC
# MAGIC Os dados de telemetria da BrasilMart — eventos de criação de pedido,
# MAGIC aprovação de pagamento, postagem, transporte e entrega — têm uma
# MAGIC característica dual: precisam alimentar tanto **dashboards operacionais
# MAGIC em tempo real** (como o monitor de atrasos do Diretor de Operações, TP1 Q3)
# MAGIC quanto **modelos analíticos batch** (como a segmentação RFM e o treinamento
# MAGIC do modelo de predição de atraso). A arquitetura **Lambda** resolve essa
# MAGIC dualidade separando os fluxos: a *batch layer* (Glue → S3 → DLT → dbt →
# MAGIC Redshift Gold) processa o histórico completo com alta confiabilidade, enquanto
# MAGIC a *speed layer* (DLT com APPLY CHANGES INTO / Structured Streaming) processa
# MAGIC eventos em near-real-time para CDC e alertas. De fato, a arquitetura atual
# MAGIC da BrasilMart **já é Lambda implícita**: o pipeline batch (Step Functions →
# MAGIC DLT → dbt) coexiste com o pipeline streaming (DLT CDC com 4 tabelas
# MAGIC `silver_*_atualizada`), e a camada de serving (Redshift Gold + Model Serving)
# MAGIC consolida ambas as visões. A arquitetura **Kappa**, por sua vez, propõe um
# MAGIC único pipeline streaming que processa tudo como eventos — eliminando a batch
# MAGIC layer e tratando o histórico como um replay do stream. Para a BrasilMart,
# MAGIC a abordagem Kappa pura seria **prematura**, por três razões: (i) o volume
# MAGIC atual (~100 mil pedidos/mês) não justifica a complexidade operacional de um
# MAGIC sistema full-streaming com garantias exactly-once; (ii) os dados-fonte são
# MAGIC CSVs estáticos do Olist que não produzem um stream nativo — a simulação de
# MAGIC streaming (TP1, notebook `02_streaming_simulado`) já evidenciou esse gap;
# MAGIC (iii) workloads como treinamento de ML (MLflow + sklearn) e dbt são
# MAGIC inerentemente batch e seriam artificialmente complicadas num paradigma
# MAGIC Kappa. **A recomendação é manter a abordagem Lambda**, que a BrasilMart já
# MAGIC pratica, evoluindo gradualmente: quando o marketplace atingir escala de
# MAGIC milhões de pedidos/mês e implementar um barramento de eventos real (Kafka
# MAGIC ou Kinesis), a speed layer do DLT Structured Streaming já estaria
# MAGIC preparada para absorver esse volume, e a migração para Kappa se tornaria
# MAGIC natural — bastando unificar batch e streaming no mesmo pipeline DLT, algo
# MAGIC que o Delta Live Tables já suporta com a API unificada de batch e streaming
# MAGIC sobre Delta Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Quadro Comparativo
# MAGIC
# MAGIC ### Data Fabric — Aplicabilidade na BrasilMart
# MAGIC
# MAGIC | Pilar do Data Fabric | Estado Atual | Com Data Fabric |
# MAGIC |---------------------|-------------|----------------|
# MAGIC | **Catálogo unificado** | Unity Catalog (Databricks) + Lake Formation (AWS) — isolados | Catálogo federado único com visão cross-platform |
# MAGIC | **Governança automatizada** | PII discovery manual (13 colunas), RLS/CLS configurados por script | Descoberta automática de PII, recomendação de políticas por ML |
# MAGIC | **Linhagem end-to-end** | Linhagem parcial (Unity Catalog dentro do Databricks; dbt docs no Redshift) | Linhagem unificada: CSV → S3 → Bronze → Silver → Gold → ML → Serving |
# MAGIC | **Descoberta semântica** | Engenheiro precisa saber em qual sistema o dado está | Busca em linguagem natural: "tabela de segmentação de clientes" |
# MAGIC | **Integração ML** | MLflow Registry isolado do catálogo de dados | Modelo registrado como asset no catálogo, com linhagem para features |
# MAGIC
# MAGIC ### Lambda vs Kappa — Dados de Telemetria
# MAGIC
# MAGIC | Critério | Lambda (recomendada) | Kappa |
# MAGIC |----------|---------------------|-------|
# MAGIC | **Complexidade** | Dois pipelines (batch + stream), mas cada um é simples | Um pipeline, mas precisa processar tudo como stream |
# MAGIC | **Adequação ao volume atual** | 100K pedidos/mês — batch é suficiente, stream complementar | Over-engineering para o volume atual |
# MAGIC | **Fontes de dados** | CSVs + batch ingest natural | CSVs não produzem stream — exigiria replay artificial |
# MAGIC | **ML training** | Batch nativo (MLflow, sklearn, dbt) | Treinamento em batch sobre streaming é complexo |
# MAGIC | **Latência** | Batch: diário / Stream (DLT CDC): near-real-time | Tudo near-real-time (quando justificável) |
# MAGIC | **Evolução futura** | DLT já unifica batch + streaming — migração gradual para Kappa | Adotar quando houver Kafka/Kinesis nativo |
# MAGIC | **Estado atual BrasilMart** | Já implementada (Step Functions + DLT batch + DLT CDC) | Não implementada |
