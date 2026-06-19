# TP1 - Parte 1.4: Defesa Técnica — Data Lakehouse e Apache Spark

## Por que Data Lakehouse para o Marketplace BrasilMart?

### O Problema das Abordagens Tradicionais

A BrasilMart opera com 9 fontes de dados heterogêneas que nenhuma arquitetura tradicional resolve sozinha:

**Data Lake puro (S3 + CSVs):**
- Aceita os 9 arquivos CSV da Olist sem transformação — ✅
- Mas não garante qualidade nem consistência entre eles — ❌
- `customer_id` em `orders.csv` não garante existência em `customers.csv` (sem integridade referencial) — ❌
- Queries SQL diretas sobre CSV são lentas e caras — ❌
- Sem controle de acesso granular: analista de marketing veria dados de pagamento indevidamente — ❌

**Data Warehouse puro (Redshift):**
- Queries SQL rápidas — ✅
- Mas requer ETL rígido e schema fixo antes de qualquer carga — ❌
- Armazenar 1 milhão de registros de geolocalização no Redshift custa 50x mais que no S3 — ❌
- Quando a BrasilMart adquirir outro marketplace, integrar schemas diferentes levará meses — ❌
- Sem suporte nativo a ML e processamento de texto de reviews — ❌

### A Solução: Data Lakehouse

O Data Lakehouse resolve os dois problemas ao mesmo tempo:

```
┌──────────────────────────────────────────────────────┐
│  CONSUMO: SQL (Redshift/Databricks), Power BI, ML    │
├──────────────────────────────────────────────────────┤
│  DELTA LAKE: Transações ACID + Schema evolution      │  ← Confiabilidade do Warehouse
├──────────────────────────────────────────────────────┤
│  GLUE CATALOG: Metadados, schemas, linhagem          │  ← Governança centralizada
├──────────────────────────────────────────────────────┤
│  AMAZON S3: Armazenamento ilimitado e barato         │  ← Flexibilidade do Data Lake
└──────────────────────────────────────────────────────┘
```

**Benefícios concretos para o e-commerce:**

| Característica | Benefício para a BrasilMart |
|---------------|------------------------------|
| **Transações ACID** | Um pipeline que atualiza pedidos não corrompe dados enquanto analistas estão consultando os mesmos |
| **Schema Evolution** | Quando o sistema de pagamentos adicionar um novo tipo (PIX, por exemplo), o schema se adapta sem quebrar pipelines existentes |
| **Time Travel** | Se um pipeline de limpeza remover dados incorretamente, voltamos ao estado anterior em minutos ("version 0 do Delta table") |
| **Acesso unificado** | Analistas de BI usam SQL via Redshift, cientistas usam PySpark via Databricks, ambos no mesmo dado |
| **Custo** | Geolocation com 1 milhão de registros: **R$ ~2/mês** no S3 vs **R$ ~80/mês** no Redshift |

---

## Por que Apache Spark (e não apenas AWS Glue)?

### O que o AWS Glue Faz Bem — e Fazemos Neste Projeto

O Glue Studio é a escolha certa para ingestão batch dos CSVs mais simples da Olist:

```python
# Glue Job: olist_orders.csv → Parquet em Bronze
# Simples, tabulare, schema bem definido — exatamente o caso de uso do Glue
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://brasilmart-raw-dev/orders/"]},
    format="csv",
    format_options={"withHeader": True, "separator": ","}
)
```

Para `olist_orders`, `olist_customers`, `olist_products` e `olist_sellers` — datasets tabulares, schema estável, volume moderado — o Glue é ideal: serverless, integrado ao Glue Catalog, sem cluster para gerenciar.

### Onde o Glue Sozinho Falha — e o Spark é Necessário

#### 1. JSON Profundamente Aninhado (Pedido Unificado)

O requisito de "ingestão de dados complexos" é atendido pela combinação dos 4 CSVs de pedidos em um único documento JSON aninhado — estrutura típica de APIs de e-commerce. O JSON gerado tem esta forma:

```
order (raiz)
  └── customer (objeto aninhado)
  └── timestamps (objeto aninhado com 5 campos)
  └── items[] (array de objetos)
       └── product_id, seller_id, price, freight...
  └── payments[] (array de objetos)
       └── type, installments, value...
  └── review (objeto aninhado com texto livre)
```

**Problema com Glue:** o Glue Studio consegue processar JSON de 1-2 níveis com o mapeamento visual. Para o JSON de pedidos (4 níveis + arrays dentro de arrays), o mapeamento visual se torna impraticável e o job gerado não é eficiente. A operação `explode` em arrays aninhados requer lógica customizada.

**Solução com PySpark (Databricks):**
```python
# PySpark lida nativamente com arrays aninhados
df = spark.read.option("multiLine", False).json(orders_json_path)

# Explodir items[] — 1 pedido → N linhas de items
df_items = df.select("order_id", "order_status", "customer.*",
                     F.explode("items").alias("item")) \
             .select("*", "item.*").drop("item")

# Explodir payments[] — N formas de pagamento por pedido
df_payments = df.select("order_id", F.explode("payments").alias("pmt")) \
                .select("order_id", "pmt.*")
```

Com PySpark, o que seria um job Glue complexo e lento vira 10 linhas de código claras, executadas em paralelo em múltiplos workers.

#### 2. Geolocation Dataset: 1 Milhão de Registros

O `olist_geolocation_dataset.csv` tem **1.000.163 registros** — o maior dataset do projeto. Ele é usado para enriquecer clientes e vendedores com coordenadas geográficas (lat/lng) a partir do CEP.

**Problema com Glue:** um join de 99.441 clientes × 1.000.163 registros de geolocalização exige que um dos datasets caiba em memória ou seja broadcast. O Glue tem limite de memória por DPU-hour e sem controle de broadcast hints pode gerar OOM ou spill massivo para disco, multiplicando o tempo de execução.

**Solução com Spark (Databricks):**
```python
# Broadcast hint para a tabela menor (customers, 99k registros)
# O Spark distribui a cópia completa de customers para cada executor
df_result = df_geo.join(
    F.broadcast(df_customers),
    on="zip_code_prefix",
    how="left"
)
# Com AQE ativado, o Spark otimiza automaticamente o join strategy
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

#### 3. Streaming Simulado — Pedidos por Ordem Cronológica

O dataset de pedidos cobre 2 anos de transações (2016-2018). Para simular um cenário de streaming (novos pedidos chegando em tempo real), processamos o dataset em ordem cronológica usando o timestamp de compra, simulando o fluxo de eventos do dia a dia do marketplace.

**Problema com Glue:** o Glue Streaming processa arquivos inteiros como micro-batch, não permite simular eventos baseados em timestamps internos do dado, e tem latência mínima de 60 segundos.

**Solução com Spark Structured Streaming:**
```python
# Simula chegada de pedidos em ordem cronológica
# usando Rate Source + join com dados históricos
df_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load("s3://brasilmart-raw-dev/orders_json/streaming/"))

# Janelas de tempo para análise de tendências de venda
df_windowed = (df_stream
    .withWatermark("purchase_timestamp", "10 minutes")
    .groupBy(window("purchase_timestamp", "1 hour"), "customer_state")
    .agg(F.count("order_id").alias("orders_per_hour"),
         F.sum("total_value").alias("gmv_per_hour")))
```

#### 4. Análise de Reviews com Processamento de Texto

O dataset de reviews tem **104.719 avaliações** com campos de texto livre (`review_comment_title`, `review_comment_message`). A análise de causa raiz de reviews negativos (NLP básico) exige tokenização, remoção de stopwords e vetorização — operações que o Glue simplesmente não suporta.

**Solução com Spark MLlib:**
```python
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF

# Pipeline de NLP para classificar reviews negativos
tokenizer = Tokenizer(inputCol="review_comment_message", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words",
                           stopWords=portugues_stopwords)
tf = HashingTF(inputCol="filtered_words", outputCol="features")

# Mesmo cluster Spark que processa os dados treina o modelo
# sem mover dados para outra plataforma
```

---

## Comparativo Quantitativo para a BrasilMart

| Operação | AWS Glue | Spark/Databricks | Vencedor |
|----------|----------|-----------------|----------|
| Ingestão olist_orders.csv (99k linhas) | ~2 min, $0.44/hora | Cluster ocioso necessário | **Glue** |
| Join customers × geolocation (1M rows) | OOM ou >30 min | ~3 min (broadcast join) | **Spark** |
| Explosão JSON aninhado (4 níveis) | Complexo, lento | ~1 min (explode nativo) | **Spark** |
| Streaming de pedidos (near-real-time) | Mín. 60s latência | ~10s latência | **Spark** |
| NLP em 100k reviews | Não suportado | Suportado (MLlib) | **Spark** |
| Custo para ETL simples/batch | Menor | Maior | **Glue** |
| Custo para processamento pesado | Alto (DPU imprevisível) | Menor (spot instances) | **Spark** |

---

## Estratégia: Dual Ingestion Complementar

A escolha não é Glue **ou** Spark — é saber usar cada um onde ele brilha:

```
olist_orders.csv ────────▶ AWS Glue Studio ──▶ Bronze (Parquet)
olist_customers.csv ─────▶ AWS Glue Studio ──▶ Bronze (Parquet)
olist_products.csv ──────▶ AWS Glue Studio ──▶ Bronze (Parquet)
olist_sellers.csv ───────▶ AWS Glue Studio ──▶ Bronze (Parquet)

orders_unified.json ─────▶ Databricks PySpark ──▶ Bronze (Delta)
olist_geolocation.csv ───▶ Databricks PySpark ──▶ Bronze (Delta)
olist_reviews.csv ───────▶ Databricks PySpark ──▶ Bronze (Delta)
streaming simulado ──────▶ Databricks Streaming ▶ Bronze (Delta)
```

O Delta Lake sobre S3 é o elemento unificador: ambas as ferramentas leem e escrevem no mesmo storage, governado pelo mesmo Glue Data Catalog e protegido pelo mesmo Lake Formation — garantindo uma única fonte de verdade para toda a plataforma BrasilMart.

---

## Conclusão

A arquitetura Data Lakehouse com ingestão dual (Glue + Spark) é a resposta técnica correta para os desafios da BrasilMart:

1. **Escalabilidade**: o dataset de geolocalização com 1 milhão de registros cresce continuamente — só o Spark suporta isso com custo controlado.
2. **Flexibilidade**: novos datasets de marketplaces adquiridos se encaixam na camada Bronze sem alterar a arquitetura Silver/Gold.
3. **Confiabilidade**: Delta Lake com ACID garante que os analistas nunca consultem dados parcialmente atualizados.
4. **Custo**: Glue serverless para ETL simples (sem cluster ocioso) + spot instances no Databricks para processamento pesado (70% de economia vs. on-demand).
5. **Competitividade**: a visão 360° do cliente — impossível com sistemas atuais — se torna viável, habilitando personalização, retenção e detecção de fraude que diferem a BrasilMart da concorrência.
