# TP1 - Parte 3.1: Configuração do Workspace Databricks

## 1. Workspace Setup

### Criação do Workspace
O workspace Databricks foi provisionado na AWS via Account Console, conectado à mesma conta AWS onde residem os buckets S3 do Data Lakehouse.

**Configurações do Workspace:**
- **Nome:** `vidaplus-data-platform`
- **Região AWS:** `us-east-1` (mesma dos buckets S3)
- **Pricing Tier:** Premium (necessário para Unity Catalog e controle de acesso)
- **Credential Configuration:** Cross-account IAM Role para acesso ao S3

### Integração com S3
O workspace foi configurado com Instance Profile que permite acesso direto aos buckets S3:

```
Buckets acessíveis:
  - s3://vidaplus-raw-dev/          (leitura)
  - s3://vidaplus-bronze-dev/       (leitura/escrita)
  - s3://vidaplus-silver-dev/       (leitura/escrita)
  - s3://vidaplus-gold-dev/         (leitura/escrita)
```

---

## 2. Clusters

### All-Purpose Cluster (Desenvolvimento)

| Parâmetro | Valor | Justificativa |
|-----------|-------|---------------|
| **Nome** | `vidaplus-dev` | Identificação clara do propósito |
| **Databricks Runtime** | 14.3 LTS (Spark 3.5.0, Scala 2.12) | LTS para estabilidade |
| **Node Type (Driver)** | m5.xlarge (4 vCPUs, 16 GB RAM) | Suficiente para desenvolvimento |
| **Node Type (Workers)** | m5.xlarge (4 vCPUs, 16 GB RAM) | Custo-benefício |
| **Workers** | 2 (fixo) | Volume de dev não exige auto-scaling |
| **Auto Termination** | 30 minutos | Evita custo de cluster ocioso |
| **Spark Config** | `spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension` | Habilita Delta Lake |
| **Python Version** | 3.10 | Compatível com pyproject.toml |
| **Libraries** | `delta-spark`, `boto3`, `pyarrow` | Dependências do projeto |

**Uso:** Notebooks interativos, exploração de dados, desenvolvimento de pipelines.

### Jobs Cluster (Produção)

| Parâmetro | Valor | Justificativa |
|-----------|-------|---------------|
| **Nome** | `vidaplus-etl-prod` | Identificação clara |
| **Databricks Runtime** | 14.3 LTS | Mesmo runtime do dev para consistência |
| **Node Type (Driver)** | m5.xlarge | Driver não precisa ser grande |
| **Node Type (Workers)** | m5.2xlarge (8 vCPUs, 32 GB RAM) | Mais memória para joins massivos |
| **Workers (Min)** | 2 | Mínimo para paralelismo |
| **Workers (Max)** | 8 | Escala conforme demanda |
| **Auto-scaling** | Habilitado | Adapta-se ao volume de dados |
| **Spot Instances** | Habilitado (workers only) | Reduz custo em até 70% |
| **Spot Fall Back** | On-Demand | Garante disponibilidade |

**Uso:** Jobs agendados de ETL, pipelines de produção, processamento batch diário.

### Diferença entre All-Purpose e Jobs Cluster

| Aspecto | All-Purpose | Jobs Cluster |
|---------|-------------|-------------|
| **Ciclo de vida** | Permanece ligado até auto-terminate | Criado ao iniciar o job, destruído ao finalizar |
| **Custo** | Maior (DBU rate mais alto + tempo ocioso) | Menor (DBU rate menor + sem tempo ocioso) |
| **Interatividade** | Sim — notebooks, REPL, exploração | Não — execução headless |
| **Compartilhamento** | Múltiplos usuários simultâneos | Um job por cluster |
| **Quando usar** | Desenvolvimento, debug, análise ad-hoc | Pipelines agendados, ETL de produção |

---

## 3. Estrutura de Notebooks no Workspace

```
/Workspace/
  └── VidaPlus/
      ├── 01_Ingestao/
      │   ├── 01_ingestao_json_aninhado      (Exames Lab - JSON aninhado → Bronze)
      │   └── 02_streaming_simulado           (Sinais Vitais IoT → Bronze)
      ├── 02_Limpeza/
      │   └── 03_limpeza_bronze               (Limpeza e qualidade na Bronze)
      ├── 03_Transformacao/                   (TPs futuros)
      │   └── (Silver layer processing)
      ├── 04_Analytics/                       (TPs futuros)
      │   └── (Gold layer aggregations)
      └── Utils/
          └── spark_helpers                   (Funções utilitárias)
```

---

## 4. Configurações Adicionais

### Instance Profile (IAM)
O Instance Profile `vidaplus-databricks-instance-profile` foi associado ao workspace com as permissões:
- `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` nos 4 buckets
- `kms:Decrypt`, `kms:GenerateDataKey` na KMS key do projeto
- `glue:GetDatabase`, `glue:GetTable`, `glue:GetPartitions` para acessar o Glue Data Catalog

### Secret Scope
Credenciais sensíveis armazenadas no Databricks Secret Scope:
- `vidaplus/aws-access-key` — chave de acesso AWS (se não usar Instance Profile)
- `vidaplus/aws-secret-key` — chave secreta AWS
