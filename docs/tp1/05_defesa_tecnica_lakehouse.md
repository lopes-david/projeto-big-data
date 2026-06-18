# TP1 - Parte 1.4: Defesa Técnica — Data Lakehouse e Apache Spark

## Por que Data Lakehouse?

### O Problema das Abordagens Tradicionais

A VidaPlus Saúde enfrenta um desafio que nenhuma arquitetura tradicional resolve sozinha:

**Data Lake puro (S3 + arquivos):**
- Aceita qualquer formato de dado (JSON do laboratório, CSV do prontuário, streaming de IoT) — ✅
- Mas não garante qualidade, consistência nem controle de acesso granular — ❌
- Consultas SQL diretas são lentas e não confiáveis sem esquema definido — ❌
- Fenômeno conhecido como "data swamp": o lago de dados vira pântano sem governança — ❌

**Data Warehouse puro (Redshift):**
- Consultas SQL rápidas e confiáveis — ✅
- Controle de acesso e auditoria robustos — ✅
- Mas exige que os dados sejam estruturados antes de serem carregados (ETL rígido) — ❌
- Custo proibitivo para armazenar terabytes de dados brutos (JSON, logs, imagens) — ❌
- Não suporta workloads de machine learning nativamente — ❌

### A Solução: Data Lakehouse

O Data Lakehouse combina a **flexibilidade do Data Lake** com a **confiabilidade do Data Warehouse** através de uma camada de metadados e transações sobre o armazenamento em nuvem (S3):

```
┌─────────────────────────────────────────────┐
│           CONSUMO (BI, SQL, ML)             │
├─────────────────────────────────────────────┤
│        DELTA LAKE (Transações ACID)         │  ← Confiabilidade do Warehouse
├─────────────────────────────────────────────┤
│     GLUE DATA CATALOG (Schema/Metadados)    │  ← Governança centralizada
├─────────────────────────────────────────────┤
│           AMAZON S3 (Armazenamento)         │  ← Flexibilidade do Data Lake
└─────────────────────────────────────────────┘
```

**Benefícios concretos para a VidaPlus:**

| Característica | Benefício para Saúde |
|---------------|---------------------|
| **Transações ACID (Delta Lake)** | Garante que uma atualização de prontuário nunca corrompa dados existentes — crítico quando vidas dependem da integridade dos dados |
| **Schema Evolution** | Quando um novo hospital é adquirido e seus dados têm campos diferentes, o schema se adapta sem quebrar pipelines existentes |
| **Time Travel** | Permite consultar o estado dos dados em qualquer ponto no passado — essencial para auditoria LGPD ("quais dados tínhamos do paciente X em 01/03/2025?") |
| **Acesso unificado** | Mesmos dados acessíveis via Spark (para ML e processamento pesado), SQL (para analistas) e BI tools (para gestores) |
| **Custo otimizado** | Armazenamento em S3 (centavos por GB) + processamento sob demanda (paga só quando usa) |

---

## Por que Apache Spark (e não apenas AWS Glue)?

### O que o AWS Glue Faz Bem

O AWS Glue é adequado para:
- ETL simples e tabulares (CSV → Parquet, renomear colunas, filtros básicos)
- Jobs com volume moderado (até centenas de GBs)
- Ingestão agendada sem necessidade de cluster permanente
- Integração nativa com Glue Data Catalog

**No TP1, usamos o Glue Studio para a ingestão batch inicial de dados tabulares simples** — como o CSV de agendamentos, onde a transformação é direta e o volume é gerenciável.

### Onde o Glue Sozinho Não Performa Bem

Para os cenários complexos da VidaPlus, o Glue apresenta limitações significativas:

#### 1. JSON Profundamente Aninhado (Dados de Laboratório)

Os resultados de laboratório chegam em JSON com múltiplos níveis de aninhamento:

```json
{
  "paciente_id": "P001",
  "ordem_exame": {
    "medico_solicitante": {...},
    "paineis": [
      {
        "nome": "Hemograma Completo",
        "resultados": [
          {
            "analito": "Hemoglobina",
            "valor": 13.5,
            "unidade": "g/dL",
            "referencia": {"min": 12.0, "max": 17.5},
            "flags": ["NORMAL"]
          },
          ...
        ]
      }
    ]
  }
}
```

**Problema com Glue:** O Glue Studio lida bem com JSON de 1-2 níveis, mas para estruturas com arrays de objetos contendo sub-arrays (como painéis → resultados → referências), o mapeamento visual se torna impraticável e o desempenho degrada. O job precisa de lógica customizada (explode de arrays, pivot, flatten) que é mais natural em PySpark.

**Solução com Spark (Databricks):**
```python
# PySpark lida nativamente com JSON aninhado
df = spark.read.json("s3://vidaplus-raw/laboratorio/")
df_exploded = (df
    .select("paciente_id", explode("ordem_exame.paineis").alias("painel"))
    .select("paciente_id", "painel.nome", explode("painel.resultados").alias("resultado"))
    .select("paciente_id", "painel.nome", "resultado.*")
)
```

#### 2. Streaming de Dados de IoT (Sinais Vitais em UTI)

Monitores de sinais vitais em UTI enviam dados a cada segundo: frequência cardíaca, pressão arterial, saturação de O2, temperatura. Com 500 leitos de UTI na rede, são **43 milhões de registros por dia**.

**Problema com Glue:** O Glue Streaming existe, mas tem latência mínima de 60 segundos (micro-batch) e não oferece controle fino sobre watermarks, janelas de tempo e state management — necessários para detectar anomalias em séries temporais de sinais vitais.

**Solução com Spark Structured Streaming (Databricks):**
```python
# Processamento near-real-time com janelas de 10 segundos
stream = (spark.readStream
    .format("json")
    .schema(sinais_vitais_schema)
    .load("s3://vidaplus-raw/iot/sinais_vitais/")
)

alertas = (stream
    .withWatermark("timestamp", "30 seconds")
    .groupBy(window("timestamp", "10 seconds"), "paciente_id", "leito")
    .agg(
        avg("freq_cardiaca").alias("fc_media"),
        max("pressao_sistolica").alias("pa_max")
    )
    .filter("fc_media > 120 OR fc_media < 50 OR pa_max > 180")
)
```

#### 3. Processamento de Grandes Volumes Históricos

Para construir a visão 360° do paciente, é necessário cruzar 5 anos de dados de 20 hospitais — **centenas de milhões de registros** envolvendo joins complexos entre tabelas de consultas, exames, prescrições e internações.

**Problema com Glue:** Jobs do Glue para joins massivos frequentemente enfrentam:
- Timeout (máximo 48h para Glue ETL)
- Out-of-memory sem controle fino de particionamento
- Custo imprevisível (DPU-hours podem escalar rapidamente)

**Solução com Spark (Databricks):**
- Controle granular de particionamento (`repartition`, `coalesce`)
- Broadcast joins para tabelas de lookup (CID-10, TISS/TUSS)
- Adaptive Query Execution (AQE) que otimiza o plano de execução em runtime
- Spot instances com auto-scaling (custo até 70% menor)
- Cache inteligente de datasets intermediários

#### 4. Machine Learning sobre Dados Clínicos

Modelos preditivos como predição de readmissão ou detecção de infecção hospitalar exigem feature engineering complexo sobre dados distribuídos.

**Problema com Glue:** Não tem integração com bibliotecas de ML. Preparar features no Glue e depois mover para outra plataforma para treinar modelos cria fricção e duplicação.

**Solução com Spark MLlib (Databricks):**
- Feature engineering e treinamento no mesmo ambiente
- MLflow integrado para tracking de experimentos
- Mesmo cluster Spark que processa os dados treina o modelo

---

## Comparativo Resumido

| Critério | AWS Glue | Apache Spark (Databricks) |
|----------|----------|--------------------------|
| **ETL simples (CSV → Parquet)** | ✅ Ideal — serverless, sem cluster | Funciona, mas overkill |
| **JSON aninhado (3+ níveis)** | ⚠️ Limitado — mapeamento visual falha | ✅ Nativo — explode/flatten |
| **Streaming (latência < 30s)** | ⚠️ Mínimo 60s micro-batch | ✅ Structured Streaming ~10s |
| **Joins massivos (100M+ rows)** | ❌ Timeout/OOM frequente | ✅ AQE + controle de partições |
| **Machine Learning** | ❌ Não suporta | ✅ MLlib + MLflow integrados |
| **Custo para ETL simples** | ✅ Menor (serverless) | ❌ Maior (cluster necessário) |
| **Custo para processamento pesado** | ❌ DPU-hours imprevisíveis | ✅ Spot instances + auto-scaling |
| **Interatividade / exploração** | ❌ Job-based, sem interação | ✅ Notebooks interativos |

---

## Conclusão: Arquitetura Dual Complementar

A estratégia adotada é **usar cada ferramenta onde ela é mais adequada**:

1. **AWS Glue** para ingestão batch de dados simples e tabulares (CSV de agendamentos, exports tabulares de sistemas legados), tirando proveito do modelo serverless e da integração nativa com o Glue Data Catalog.

2. **Apache Spark via Databricks** para todo processamento que envolve:
   - Dados complexos (JSON aninhado, schemas heterogêneos)
   - Grandes volumes (joins de centenas de milhões de registros)
   - Streaming (dados de IoT em near-real-time)
   - Machine learning (predição de readmissão, detecção de anomalias)

Esta abordagem dual maximiza o custo-benefício: não pagamos por um cluster Spark para tarefas simples que o Glue resolve por centavos, mas não limitamos nosso processamento à capacidade do Glue quando a complexidade exige o poder do Spark distribuído.

O Data Lakehouse, materializado pelo Delta Lake sobre S3, é o elemento que une as duas ferramentas: ambas leem e escrevem no mesmo formato, no mesmo storage, governado pelo mesmo catálogo — garantindo consistência, rastreabilidade e uma única fonte de verdade para toda a rede VidaPlus.
