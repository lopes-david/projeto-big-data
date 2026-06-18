# TP1 - Parte 1.3: Arquitetura da Solução

## Visão Geral

A arquitetura integra serviços AWS (S3, Glue, Lake Formation, Redshift) com Databricks, formando um Data Lakehouse híbrido que suporta ingestão batch e streaming, processamento distribuído com Spark, governança centralizada e consumo analítico.

---

## Diagrama da Arquitetura

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        PLATAFORMA DE DADOS VIDAPLUS SAÚDE                           │
│                        Arquitetura Data Lakehouse Híbrida                            │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ╔══════════════════╗                                                               │
│  ║  FONTES DE DADOS ║                                                               │
│  ╚══════════════════╝                                                               │
│                                                                                     │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────┐                │
│  │   HIS    │ │   LIS    │ │   RIS    │ │Agendamento│ │IoT (Sinais │                │
│  │Prontuário│ │Laboratório│ │Radiologia│ │  (REST)  │ │  Vitais)   │                │
│  │ (HL7/CSV)│ │  (JSON)  │ │ (DICOM)  │ │  (JSON)  │ │ (Streaming)│                │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └─────┬──────┘                │
│       │             │            │             │              │                       │
│       ▼             ▼            ▼             ▼              ▼                       │
│  ╔══════════════════════════════════════════════════════════════╗                    │
│  ║                    CAMADA DE INGESTÃO                        ║                    │
│  ╠══════════════════════════════════════════════════════════════╣                    │
│  ║                                                              ║                    │
│  ║  ┌─────────────────────┐    ┌─────────────────────────┐      ║                    │
│  ║  │   AWS Glue Studio   │    │    Databricks Notebook   │      ║                    │
│  ║  │   (Batch ETL Job)   │    │  (PySpark - Streaming)   │      ║                    │
│  ║  │                     │    │                           │      ║                    │
│  ║  │ • CSV → Parquet     │    │ • JSON aninhado → Delta   │      ║                    │
│  ║  │ • Ingestão agendada │    │ • Structured Streaming    │      ║                    │
│  ║  │ • Schema inference  │    │ • Schema evolution        │      ║                    │
│  ║  └─────────┬───────────┘    └────────────┬────────────┘      ║                    │
│  ║            │                              │                   ║                    │
│  ╚════════════╪══════════════════════════════╪═══════════════════╝                    │
│               │                              │                                        │
│               ▼                              ▼                                        │
│  ╔══════════════════════════════════════════════════════════════╗                    │
│  ║                  AMAZON S3 — DATA LAKEHOUSE                  ║                    │
│  ╠══════════════════════════════════════════════════════════════╣                    │
│  ║                                                              ║                    │
│  ║  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       ║                    │
│  ║  │  s3://raw/    │  │s3://bronze/  │  │ s3://silver/  │       ║                    │
│  ║  │              │  │              │  │              │       ║                    │
│  ║  │ Dados brutos │  │ Parquet/Delta│  │ Dados limpos │       ║                    │
│  ║  │ originais    │──▶│ sem limpeza  │──▶│ padronizados │       ║                    │
│  ║  │ (JSON,CSV,   │  │ com metadata │  │ deduplicados │       ║                    │
│  ║  │  HL7)        │  │              │  │ tipados      │       ║                    │
│  ║  └──────────────┘  └──────────────┘  └──────┬───────┘       ║                    │
│  ║                                              │                ║                    │
│  ║                                    ┌─────────▼────────┐      ║                    │
│  ║                                    │   s3://gold/      │      ║                    │
│  ║                                    │                   │      ║                    │
│  ║                                    │ Tabelas analíticas│      ║                    │
│  ║                                    │ agregações, KPIs  │      ║                    │
│  ║                                    │ visão 360° paciente│      ║                    │
│  ║                                    └─────────┬─────────┘      ║                    │
│  ║                                              │                ║                    │
│  ╚══════════════════════════════════════════════╪════════════════╝                    │
│                                                 │                                     │
│  ╔══════════════════════════════════════════════╪════════════════╗                    │
│  ║              PROCESSAMENTO (SPARK)           │                ║                    │
│  ╠══════════════════════════════════════════════╪════════════════╣                    │
│  ║                                              │                ║                    │
│  ║  ┌─────────────────────────────────────┐     │                ║                    │
│  ║  │         DATABRICKS WORKSPACE        │     │                ║                    │
│  ║  │                                     │     │                ║                    │
│  ║  │  ┌───────────┐  ┌───────────────┐   │     │                ║                    │
│  ║  │  │All-Purpose│  │  Jobs Cluster  │   │     │                ║                    │
│  ║  │  │  Cluster  │  │  (Produção)    │   │     │                ║                    │
│  ║  │  │(Dev/Expl.)│  │  Auto-scaling  │   │     │                ║                    │
│  ║  │  └───────────┘  └───────────────┘   │     │                ║                    │
│  ║  │                                     │     │                ║                    │
│  ║  │  Notebooks PySpark:                 │     │                ║                    │
│  ║  │  • Ingestão JSON aninhado           │     │                ║                    │
│  ║  │  • Streaming simulado               │     │                ║                    │
│  ║  │  • Limpeza e transformação          │     │                ║                    │
│  ║  │  • Agregações analíticas            │     │                ║                    │
│  ║  └─────────────────────────────────────┘     │                ║                    │
│  ║                                              │                ║                    │
│  ╚══════════════════════════════════════════════╪════════════════╝                    │
│                                                 │                                     │
│  ╔══════════════════════════════════════════════╪════════════════╗                    │
│  ║                    CONSUMO                   │                ║                    │
│  ╠══════════════════════════════════════════════╪════════════════╣                    │
│  ║                                              │                ║                    │
│  ║  ┌──────────────┐  ┌──────────────┐  ┌──────▼───────┐       ║                    │
│  ║  │  Power BI /  │  │  Databricks  │  │   Amazon     │       ║                    │
│  ║  │   Tableau    │  │     SQL      │  │  Redshift    │       ║                    │
│  ║  │              │  │  Warehouse   │  │  Serverless  │       ║                    │
│  ║  │ Dashboards   │  │  Consultas   │  │              │       ║                    │
│  ║  │ operacionais │  │  ad-hoc      │  │ Queries SQL  │       ║                    │
│  ║  └──────────────┘  └──────────────┘  │ performáticas│       ║                    │
│  ║                                      └──────────────┘       ║                    │
│  ╚══════════════════════════════════════════════════════════════╝                    │
│                                                                                     │
│  ╔══════════════════════════════════════════════════════════════╗                    │
│  ║              GOVERNANÇA E SEGURANÇA (TRANSVERSAL)            ║                    │
│  ╠══════════════════════════════════════════════════════════════╣                    │
│  ║                                                              ║                    │
│  ║  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       ║                    │
│  ║  │ AWS Lake     │  │  AWS Glue    │  │   AWS IAM    │       ║                    │
│  ║  │ Formation    │  │Data Catalog  │  │  + KMS       │       ║                    │
│  ║  │              │  │              │  │              │       ║                    │
│  ║  │ • Row-level  │  │ • Metadados  │  │ • Roles      │       ║                    │
│  ║  │   security   │  │ • Schemas    │  │ • Policies   │       ║                    │
│  ║  │ • Column-    │  │ • Linhagem   │  │ • Encryption │       ║                    │
│  ║  │   level sec. │  │ • Partições  │  │   (SSE-KMS)  │       ║                    │
│  ║  │ • Audit logs │  │              │  │              │       ║                    │
│  ║  └──────────────┘  └──────────────┘  └──────────────┘       ║                    │
│  ║                                                              ║                    │
│  ╚══════════════════════════════════════════════════════════════╝                    │
│                                                                                     │
│  ╔══════════════════════════════════════════════════════════════╗                    │
│  ║              ORQUESTRAÇÃO (TPs Futuros)                      ║                    │
│  ╠══════════════════════════════════════════════════════════════╣                    │
│  ║  ┌──────────────────────────────────────────────────┐       ║                    │
│  ║  │              Apache Airflow (MWAA)                │       ║                    │
│  ║  │  DAGs para orquestrar pipelines batch e alertas   │       ║                    │
│  ║  └──────────────────────────────────────────────────┘       ║                    │
│  ╚══════════════════════════════════════════════════════════════╝                    │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Descrição dos Componentes

### 1. Fontes de Dados
| Fonte | Formato | Protocolo | Tipo de Ingestão |
|-------|---------|-----------|-----------------|
| HIS (Prontuário Eletrônico) | CSV, HL7 | Export em lote (SFTP) | Batch (diário) |
| LIS (Laboratório) | JSON aninhado | API REST | Batch (horário) |
| RIS (Radiologia) | DICOM metadata (JSON) | API REST | Batch (horário) |
| Sistema de Agendamento | JSON | API REST | Batch (a cada 15 min) |
| IoT - Sinais Vitais (UTI) | JSON streaming | MQTT → Kinesis | Near-real-time |

### 2. Camada de Ingestão (Dual)
- **AWS Glue Studio**: para ingestão batch de dados tabulares simples (CSV de prontuários, agendamentos). Job visual com schedule via EventBridge. Salva em Parquet no S3.
- **Databricks (PySpark)**: para ingestão de dados complexos — JSON com múltiplos níveis de aninhamento (resultados de laboratório com painéis e sub-painéis), e streaming simulado de dispositivos IoT. Utiliza Structured Streaming e Delta Lake.

### 3. Armazenamento (Amazon S3)

| Bucket | Camada | Formato | Retenção | Classe de Storage |
|--------|--------|---------|----------|------------------|
| `vidaplus-raw` | Raw | Original (JSON, CSV, HL7) | 7 anos (regulatório) | S3 Standard → Glacier (90 dias) |
| `vidaplus-bronze` | Bronze | Parquet / Delta | 3 anos | S3 Standard → IA (90 dias) |
| `vidaplus-silver` | Silver | Delta Lake | 2 anos | S3 Standard → IA (180 dias) |
| `vidaplus-gold` | Gold | Delta Lake | 1 ano (dados vivos) | S3 Standard |

### 4. Processamento — Databricks Workspace

| Tipo de Cluster | Uso | Configuração |
|----------------|-----|-------------|
| **All-Purpose** | Desenvolvimento, exploração, notebooks interativos | 1 driver + 2 workers (m5.xlarge), auto-terminate 30 min |
| **Jobs Cluster** | Pipelines de produção, ETL agendado | Auto-scaling 2-8 workers (m5.2xlarge), spot instances |

### 5. Consumo Analítico
- **Amazon Redshift Serverless**: consultas SQL de alta performance sobre dados Gold. Conecta-se ao S3 via Redshift Spectrum para consultas federadas.
- **Databricks SQL Warehouse**: consultas ad-hoc e notebooks de análise exploratória.
- **Power BI / Tableau**: dashboards operacionais conectados via JDBC/ODBC ao Redshift ou Databricks SQL.

### 6. Governança e Segurança
- **AWS Lake Formation**: controle de acesso granular (row-level e column-level security), registro de permissões centralizado.
- **AWS Glue Data Catalog**: metadados, schemas, partições e linhagem de dados.
- **AWS IAM + KMS**: autenticação, autorização e criptografia (SSE-KMS para dados em repouso, TLS para dados em trânsito).

### 7. Orquestração (TPs Futuros)
- **Amazon MWAA (Managed Airflow)**: DAGs Python para orquestrar pipelines, com alertas via SNS em caso de falha.

---

## Fluxo de Dados End-to-End

```
1. EXTRAÇÃO       2. INGESTÃO          3. BRONZE           4. SILVER           5. GOLD
   Sistemas  ──▶  Glue/Databricks ──▶  Parquet/Delta  ──▶  Dados limpos  ──▶  Tabelas
   Hospitalares    (batch/stream)       (dados brutos       (padronizados       analíticas
                                         convertidos)        deduplicados)       (KPIs, 360°)
                                              │                    │                  │
                                              └────────────────────┴──────────────────┘
                                                    Glue Data Catalog (metadados)
                                                    Lake Formation (acesso)
```

---

## Decisões Arquiteturais

| Decisão | Justificativa |
|---------|--------------|
| S3 como storage central (não HDFS) | Custo 10x menor, durabilidade superior, integração nativa com Glue/Databricks/Redshift |
| Dual ingestion (Glue + Databricks) | Glue para ETL simples e barato; Databricks para processamento complexo que justifica Spark |
| Delta Lake (não Parquet puro) | ACID transactions, schema evolution e time travel — críticos para dados de saúde |
| Redshift Serverless (não provisioned) | Pay-per-query, sem cluster ocioso; adequado para padrão de uso intermitente de analistas |
| Lake Formation (não IAM puro) | Controle column-level necessário para LGPD; IAM sozinho não oferece essa granularidade |
