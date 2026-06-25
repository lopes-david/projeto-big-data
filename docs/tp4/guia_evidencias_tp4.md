# TP4 — Guia de Evidências (Prints e Telas)

## Como usar este guia
Cada seção indica: **onde entrar**, **o que fazer**, e **o que printar**.
Numere os prints como `TP4-XX_descricao.png` para organização.

---

# ATIVIDADE 1: DevOps e CI/CD Robusto

---

## 1.1 CI/CD AWS — CodePipeline + CodeBuild

### Passo 1: Criar o Secret no Secrets Manager

**Onde**: Console AWS → Serviço: **Secrets Manager** → região `sa-east-1`

1. Clicar **Store a new secret**
2. Secret type: **Other type of secret**
3. Key/value pairs:
   - `host` → `default-workgroup.234828142988.sa-east-1.redshift-serverless.amazonaws.com`
   - `user` → `admin`
   - `password` → (sua senha do Redshift)
4. Secret name: `pb-brasilmart/redshift`
5. Clicar **Store**

**PRINT TP4-01**: Tela do Secrets Manager mostrando o secret `pb-brasilmart/redshift` criado (NÃO mostrar os valores)

---

### Passo 2: Criar o CodeBuild — dbt

**Onde**: Console AWS → Serviço: **CodeBuild** → região `sa-east-1`

1. Clicar **Create build project**
2. Preencher:
   - Project name: `pb-brasilmart-dbt-build`
   - Description: `TP4 — Build e deploy do projeto dbt no Redshift`
   - Source: **No source** (virá do CodePipeline)
   - Environment:
     - Image: `aws/codebuild/amazonlinux2-x86_64-standard:5.0`
     - Compute: `EC2` → `BUILD_GENERAL1_SMALL` (3 GB, 2 vCPU)
     - Service role: **New service role** → nome: `CodeBuildServiceRole-pb-brasilmart`
   - Buildspec: **Insert build commands** → colar conteúdo de `buildspec_dbt.yml`
     OU **Use a buildspec file** → path: `infra/aws/codepipeline/buildspec_dbt.yml`
3. Clicar **Create build project**

**PRINT TP4-02**: Tela de configuração do projeto CodeBuild `pb-brasilmart-dbt-build` (visão geral após criação)

---

### Passo 3: Criar o CodeBuild — Infra (Terraform)

**Onde**: Console AWS → Serviço: **CodeBuild** → região `sa-east-1`

Repetir o Passo 2 com:
- Project name: `pb-brasilmart-infra-build`
- Description: `TP4 — Deploy de infraestrutura AWS via Terraform`
- Buildspec: conteúdo de `buildspec_infra.yml`

**PRINT TP4-03**: Tela do CodeBuild mostrando os 2 projetos criados (lista)

---

### Passo 4: Criar a CodeStar Connection (GitHub)

**Onde**: Console AWS → Serviço: **Developer Tools** → **Settings** → **Connections**

1. Clicar **Create connection**
2. Provider: **GitHub**
3. Connection name: `pb-brasilmart-github`
4. Clicar **Connect to GitHub** → autorizar no GitHub
5. Copiar o ARN da connection criada

**PRINT TP4-04**: Tela da Connection mostrando status **Available** e o ARN

---

### Passo 5: Criar o CodePipeline

**Onde**: Console AWS → Serviço: **CodePipeline** → região `sa-east-1`

1. Clicar **Create pipeline**
2. Pipeline name: `pb-brasilmart-cicd`
3. Service role: **New service role**
4. **Source stage**:
   - Source provider: **AWS CodeStar Connections (GitHub)**
   - Connection: selecionar `pb-brasilmart-github`
   - Repository: `lopes-david/tp1`
   - Branch: `main`
   - Output artifact: `SourceArtifact`
5. **Build stage** (primeiro):
   - Build provider: **AWS CodeBuild**
   - Project name: `pb-brasilmart-infra-build`
   - Nomear o stage: `Deploy_Infra`
6. Adicionar mais um stage **Build_dbt**:
   - Build provider: **AWS CodeBuild**
   - Project name: `pb-brasilmart-dbt-build`
7. **Skip deploy stage** (o build já faz o deploy)
8. Clicar **Create pipeline**

**PRINT TP4-05**: Visão do pipeline com os 3 stages (Source → Deploy_Infra → Build_dbt) — tela de visualização do pipeline

**PRINT TP4-06**: Detalhe do stage Source mostrando GitHub e branch main

---

### Passo 6: Executar o Pipeline

O pipeline executa automaticamente ao criar. Se não:
1. Clicar **Release change** no topo da página

**PRINT TP4-07**: Pipeline em execução (ou concluído) mostrando os status dos stages (verde = sucesso, azul = em andamento)

**PRINT TP4-08**: Clicar no stage `Build_dbt` → **View in CodeBuild** → aba **Build logs** mostrando a saída do `dbt run`

---

## 1.2 CD Databricks — Asset Bundle (DABs)

### Passo 1: Validar o bundle localmente

**Onde**: Terminal local (com Databricks CLI instalado)

```bash
cd /home/davidl/projeto-big-data
databricks bundle validate --target dev
```

**PRINT TP4-09**: Saída do terminal mostrando `Validation OK`

---

### Passo 2: Deploy para dev

```bash
databricks bundle deploy --target dev
```

**PRINT TP4-10**: Saída do terminal mostrando os recursos deployados (pipelines + job)

---

### Passo 3: Verificar no Databricks

**Onde**: Databricks → sidebar **Workflows** → aba **Jobs**

1. Procurar o job: `pb-brasilmart-silver-to-gold-dev`
2. Clicar nele → ver as tasks

**PRINT TP4-11**: Tela do Job mostrando as 4 tasks (Executar_DLT, Validacao, Otimizar_Gold, Alerta_Falha) com o grafo de dependências

---

### Passo 4: Verificar pipelines DLT

**Onde**: Databricks → sidebar **Workflows** → aba **Delta Live Tables**

1. Procurar: `pb-brasilmart-silver-dev`

**PRINT TP4-12**: Tela do pipeline DLT mostrando o nome e que foi criado via bundle (tag `deploy: dabs`)

---

### Passo 5 (opcional): Deploy para prod

```bash
databricks bundle deploy --target prod
```

**PRINT TP4-13**: Saída do terminal mostrando deploy prod

---

## 1.3 Monitoramento e Alertas — CloudWatch + SNS

### Passo 1: Executar o script

**Onde**: Terminal local

```bash
cd /home/davidl/projeto-big-data
bash infra/aws/setup_monitoring.sh
```

**PRINT TP4-14**: Saída do terminal mostrando os 4 passos concluídos

---

### Passo 2: Confirmar inscrição SNS

**Onde**: E-mail (david.lopes@al.infnet.edu.br)

1. Abrir o e-mail do AWS SNS com assunto "AWS Notification - Subscription Confirmation"
2. Clicar **Confirm subscription**

**PRINT TP4-15**: E-mail de confirmação do SNS recebido

---

### Passo 3: Verificar o tópico SNS

**Onde**: Console AWS → Serviço: **SNS** → região `sa-east-1` → **Topics**

1. Clicar em `pb-brasilmart-alertas`
2. Ver a subscription confirmada

**PRINT TP4-16**: Tela do SNS Topic mostrando a subscription com status **Confirmed**

---

### Passo 4: Verificar os alarmes CloudWatch

**Onde**: Console AWS → Serviço: **CloudWatch** → região `sa-east-1` → **Alarms** → **All alarms**

1. Ver os 2 alarmes:
   - `pb-brasilmart-stepfunctions-falha`
   - `pb-brasilmart-stepfunctions-timeout`

**PRINT TP4-17**: Lista de alarmes mostrando os 2 alarmes com estado OK (ou INSUFFICIENT_DATA)

**PRINT TP4-18**: Clicar em `pb-brasilmart-stepfunctions-falha` → detalhe mostrando:
- Métrica: `ExecutionsFailed`
- Threshold: `>= 1`
- Actions: SNS `pb-brasilmart-alertas`

---

### Passo 5: Testar o alarme (opcional mas recomendado)

**Onde**: Terminal local

```bash
aws cloudwatch set-alarm-state \
  --alarm-name "pb-brasilmart-stepfunctions-falha" \
  --state-value ALARM \
  --state-reason "Teste manual TP4" \
  --region sa-east-1
```

**PRINT TP4-19**: E-mail recebido com o alerta do CloudWatch (assunto: "ALARM: pb-brasilmart-stepfunctions-falha")

---

# ATIVIDADE 2: Segurança e Governança

---

## 2.1 Descoberta PII — Glue Data Catalog

### Passo 1: Executar o script de tagging

**Onde**: Terminal local

```bash
bash infra/aws/setup_pii_tagging.sh
```

**PRINT TP4-20**: Saída do terminal mostrando as 5 tabelas taggeadas

---

### Passo 2: Verificar tags no Glue

**Onde**: Console AWS → Serviço: **AWS Glue** → **Data Catalog** → **Databases** → `pb_bronze_brasilmart` → **Tables**

1. Clicar na tabela `customers`
2. Ir para aba **Table properties** → ver `pii_detected: true`
3. Ir para aba **Schema** → ver os Comments das colunas (`PII:IDENTIFICADOR_DIRETO`)

**PRINT TP4-21**: Aba Schema da tabela `customers` mostrando Comments PII nas colunas

**PRINT TP4-22**: Aba Table properties mostrando `pii_detected: true` e `pii_classification: LGPD_dados_pessoais`

---

### Passo 3: Executar notebook no Databricks

**Onde**: Databricks → Workspace → importar `tp4_02_descoberta_pii.py`

1. Executar todas as cells
2. Ver a tabela do inventário PII (cell 3)
3. Ver o scan programático (cell 5)

**PRINT TP4-23**: Output da cell do inventário PII (tabela com 13 linhas)

**PRINT TP4-24**: Output do scan mostrando `[PII] customers.customer_id → CPF_HASH`

---

## 2.2 Permissões Finas — Column-Level Security (Lake Formation)

### Passo 1: Executar o script

**Onde**: Terminal local

```bash
bash infra/aws/setup_column_security.sh
```

**PRINT TP4-25**: Saída do terminal mostrando a tabela resumo com colunas visíveis/bloqueadas

---

### Passo 2: Verificar a IAM Role

**Onde**: Console AWS → Serviço: **IAM** → **Roles** → buscar `pb-brasilmart-analista-jr`

1. Clicar na role
2. Aba **Permissions** → ver a policy `PBAnalistaJrAthenaAccess`
3. Aba **Tags** → ver `cargo: analista_junior`

**PRINT TP4-26**: Tela da IAM Role mostrando nome, policies e tags

---

### Passo 3: Verificar permissões no Lake Formation

**Onde**: Console AWS → Serviço: **Lake Formation** → **Data permissions**

1. Filtrar por Principal: `pb-brasilmart-analista-jr`
2. Ver as permissões com `ColumnWildcard` e `ExcludedColumnNames`

**PRINT TP4-27**: Lista de permissões do Lake Formation mostrando as exclusões de coluna (customers sem customer_id, sellers sem seller_id, etc.)

---

### Passo 4: Testar no Athena (como Analista Jr.)

**Onde**: Console AWS → Serviço: **Athena** → região `sa-east-1`

> **NOTA**: Para testar como Analista Jr., use **Switch Role** no Console:
> Menu do usuário (canto superior direito) → Switch Role → Account: 234828142988 → Role: pb-brasilmart-analista-jr

Query que FUNCIONA:
```sql
SELECT customer_zip_code_prefix, customer_city, customer_state
FROM pb_bronze_brasilmart.customers LIMIT 10;
```

**PRINT TP4-28**: Resultado da query com 3 colunas visíveis

Query que FALHA:
```sql
SELECT customer_id FROM pb_bronze_brasilmart.customers LIMIT 10;
```

**PRINT TP4-29**: Mensagem de erro `AccessDeniedException: Insufficient Lake Formation permission(s)`

---

## 2.3 Row-Level Security + Linhagem — Unity Catalog

### Passo 1: Criar o grupo Regiao_Norte

**Onde**: Databricks → **Admin Settings** (ícone engrenagem) → **Identity and access** → **Groups**

1. Clicar **Add group**
2. Nome: `Regiao_Norte`
3. Adicionar um membro de teste (pode ser seu próprio usuário para demonstrar)

**PRINT TP4-30**: Tela mostrando o grupo `Regiao_Norte` criado com membros

---

### Passo 2: Executar notebook RLS

**Onde**: Databricks → Workspace → importar `tp4_04_rls_lineage.py`

1. Executar as cells 1.2 a 1.6 (CREATE FUNCTION + ALTER TABLE SET ROW FILTER)
2. Executar cell 1.7 (contagem por estado — visão admin)
3. Executar cell 1.8 (simulação Regiao_Norte)

**PRINT TP4-31**: Output do CREATE FUNCTION mostrando sucesso

**PRINT TP4-32**: Output da contagem admin mostrando TODAS as regiões

**PRINT TP4-33**: Output da simulação Regiao_Norte mostrando APENAS estados AM, PA, AC, RO, RR, AP, TO

---

### Passo 3: Verificar Row Filter na tabela

**Onde**: Databricks → **Catalog** (sidebar) → `pb_brasilmart` → `silver` → `orders_enriched`

1. Clicar na tabela `orders_enriched`
2. Aba **Details** → campo **Row filter** deve mostrar `filtro_regiao_norte`

**PRINT TP4-34**: Aba Details da tabela mostrando o Row Filter ativo

---

### Passo 4: Linhagem de Dados

**Onde**: Databricks → **Catalog** → `pb_brasilmart` → `silver` → `orders`

1. Clicar na tabela `orders`
2. Aba **Lineage**
3. Ver o grafo: `bronze.orders` → `silver.orders` → `silver.orders_enriched`

**PRINT TP4-35**: Aba Lineage mostrando o grafo de dependências upstream/downstream

4. Clicar na coluna `tempo_total_seg` (se disponível) para ver linhagem column-level

**PRINT TP4-36**: Linhagem da coluna `tempo_total_seg` mostrando origem nas colunas Bronze

---

### Passo 5: Linhagem da tabela items

**Onde**: Databricks → **Catalog** → `pb_brasilmart` → `silver` → `items`

1. Aba **Lineage** → ver `bronze.items` → `silver.items` → `silver.items_enriched`

**PRINT TP4-37**: Lineage da tabela items mostrando `total_item_value` derivado de `price + freight_value`

---

# ATIVIDADE 3: MLOps Base

---

## 3.1 MLflow Tracking + Model Registry (sklearn)

### Passo 1: Executar notebook de treino SparkML

**Onde**: Databricks → Workspace → importar `tp4_05_mlops_mlflow.py`

1. Executar todas as cells
2. Esperar os 3 runs completarem

**PRINT TP4-38**: Output da cell de comparação mostrando a tabela com métricas dos 3 runs (Baseline, L1 Balanced, ElasticNet)

---

### Passo 2: Executar notebook sklearn + serving

**Onde**: Databricks → Workspace → importar `tp4_06_model_registry_serving.py`

1. Executar até a cell 4.1 (treino sklearn)
2. Executar cells 5-6 (registry)

**PRINT TP4-39**: Output mostrando métricas do run sklearn (AUC-ROC, F1, confusion matrix)

**PRINT TP4-40**: Output mostrando modelo registrado no Model Registry com versão e status

---

### Passo 3: Verificar Experiment no MLflow UI

**Onde**: Databricks → sidebar **Experiments** → `pb-brasilmart-predicao-atraso`

1. Ver a lista de runs (4 runs: 3 SparkML + 1 sklearn)
2. Selecionar todos → **Compare**

**PRINT TP4-41**: Tela do Experiment mostrando os 4 runs com métricas

**PRINT TP4-42**: Tela de comparação dos runs (gráfico de métricas lado a lado)

---

### Passo 4: Verificar um Run individual

**Onde**: Na lista de Experiments → clicar no run `sklearn_logistic_regression_final`

1. Aba **Parameters** → ver todos os hiperparâmetros logados
2. Aba **Metrics** → ver AUC-ROC, accuracy, F1, precision, recall
3. Aba **Artifacts** → ver `modelo_sklearn/`, `feature_importances.txt`, `confusion_matrix.txt`

**PRINT TP4-43**: Aba Parameters do run sklearn

**PRINT TP4-44**: Aba Metrics do run sklearn

**PRINT TP4-45**: Aba Artifacts mostrando o modelo serializado + arquivos de texto

---

### Passo 5: Verificar Model Registry

**Onde**: Databricks → sidebar **Models** → `pb-brasilmart-predicao-atraso`

1. Ver versões registradas
2. Ver descrição, tags (`flavor: sklearn`, `servable: true`)
3. Clicar na versão sklearn → ver o link para o run de origem

**PRINT TP4-46**: Tela do Model Registry mostrando versões e tags

**PRINT TP4-47**: Detalhe da versão sklearn mostrando descrição e métricas

---

## 3.2 Model Serving — Endpoint REST

### Passo 1: Criar o endpoint

**Onde**: Databricks → continuar executando `tp4_06_model_registry_serving.py`

1. Executar cells 7.1 (criar endpoint) e 7.2 (verificar status)
2. Aguardar o endpoint ficar READY (pode levar 5-10 minutos)

**PRINT TP4-48**: Output mostrando "Endpoint criado com sucesso" ou "Endpoint PRONTO!"

---

### Passo 2: Verificar no Console Databricks

**Onde**: Databricks → sidebar **Serving**

1. Clicar em `pb-brasilmart-atraso-endpoint`
2. Ver status: **Ready**
3. Ver modelo servido, workload size, scale-to-zero

**PRINT TP4-49**: Tela do Serving Endpoint mostrando status Ready e configuração

---

### Passo 3: Testar a inferência

**Onde**: Databricks → continuar o notebook cell 7.3

OU diretamente na tela do Serving Endpoint:
1. Aba **Test** (no Console do Serving)
2. Colar o JSON de teste:
```json
{
  "dataframe_records": [{
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
    "seller_state_enc": 10.0
  }]
}
```
3. Clicar **Send request**

**PRINT TP4-50**: Resposta da inferência mostrando `{"predictions": [0]}` ou `[1]`

---

# ATIVIDADE 4: Monitoramento

---

## 4.1 Monitoramento de Recursos

### Passo 1: Executar notebook de monitoramento

**Onde**: Databricks → Workspace → importar `tp4_07_monitoramento.py`

1. Executar todas as cells

**PRINT TP4-51**: Output do inventário Unity Catalog (tabelas Bronze/Silver/Gold com contagens)

**PRINT TP4-52**: Output do MLflow Experiment (lista de runs com métricas)

**PRINT TP4-53**: Output do Model Serving endpoint (status, modelo servido)

**PRINT TP4-54**: Output do inventário completo (23 notebooks TP1→TP4)

---

# CHECKLIST DE PRINTS

| Print | Onde | O que mostra |
|-------|------|-------------|
| TP4-01 | Secrets Manager | Secret `pb-brasilmart/redshift` criado |
| TP4-02 | CodeBuild | Projeto `pb-brasilmart-dbt-build` |
| TP4-03 | CodeBuild | Lista com 2 projetos |
| TP4-04 | Developer Tools | CodeStar Connection Available |
| TP4-05 | CodePipeline | Pipeline com 3 stages |
| TP4-06 | CodePipeline | Detalhe do stage Source (GitHub) |
| TP4-07 | CodePipeline | Execução com status dos stages |
| TP4-08 | CodeBuild | Build logs do `dbt run` |
| TP4-09 | Terminal | `databricks bundle validate` OK |
| TP4-10 | Terminal | `databricks bundle deploy` output |
| TP4-11 | Databricks Workflows | Job com 4 tasks (grafo) |
| TP4-12 | Databricks DLT | Pipeline criado via bundle |
| TP4-13 | Terminal | Deploy prod (opcional) |
| TP4-14 | Terminal | Script monitoring executado |
| TP4-15 | E-mail | Confirmação SNS |
| TP4-16 | SNS Console | Subscription Confirmed |
| TP4-17 | CloudWatch | 2 alarmes listados |
| TP4-18 | CloudWatch | Detalhe do alarme (métrica + threshold) |
| TP4-19 | E-mail | Alerta CloudWatch recebido |
| TP4-20 | Terminal | Script PII tagging executado |
| TP4-21 | Glue Console | Schema customers com Comments PII |
| TP4-22 | Glue Console | Table properties com pii_detected |
| TP4-23 | Databricks | Inventário PII (13 colunas) |
| TP4-24 | Databricks | Scan PII detectando colunas |
| TP4-25 | Terminal | Script column security executado |
| TP4-26 | IAM Console | Role analista-jr com policy e tags |
| TP4-27 | Lake Formation | Permissões com ExcludedColumnNames |
| TP4-28 | Athena | Query OK (colunas não-PII) |
| TP4-29 | Athena | Query FALHA (AccessDeniedException) |
| TP4-30 | Databricks Admin | Grupo Regiao_Norte |
| TP4-31 | Databricks | CREATE FUNCTION sucesso |
| TP4-32 | Databricks | Contagem admin (todas regiões) |
| TP4-33 | Databricks | Contagem filtrada (só Norte) |
| TP4-34 | Catalog Explorer | Row Filter ativo na tabela |
| TP4-35 | Catalog Explorer | Lineage grafo (Bronze→Silver→Enriched) |
| TP4-36 | Catalog Explorer | Lineage coluna tempo_total_seg |
| TP4-37 | Catalog Explorer | Lineage items (total_item_value) |
| TP4-38 | Databricks | Comparação 3 runs SparkML |
| TP4-39 | Databricks | Métricas run sklearn |
| TP4-40 | Databricks | Modelo registrado no Registry |
| TP4-41 | MLflow UI | Experiment com 4 runs |
| TP4-42 | MLflow UI | Comparação de runs (gráfico) |
| TP4-43 | MLflow UI | Parameters do run sklearn |
| TP4-44 | MLflow UI | Metrics do run sklearn |
| TP4-45 | MLflow UI | Artifacts (modelo + arquivos) |
| TP4-46 | Model Registry | Versões e tags |
| TP4-47 | Model Registry | Detalhe versão sklearn |
| TP4-48 | Databricks | Endpoint criado com sucesso |
| TP4-49 | Serving Console | Endpoint Ready + config |
| TP4-50 | Serving Console | Resposta da inferência REST |
| TP4-51 | Databricks | Inventário Unity Catalog |
| TP4-52 | Databricks | MLflow experiment runs |
| TP4-53 | Databricks | Model Serving status |
| TP4-54 | Databricks | Inventário 23 notebooks |
