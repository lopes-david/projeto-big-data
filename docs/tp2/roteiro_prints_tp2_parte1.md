# TP2 — Roteiro de Prints e Evidências (Itens 1.1 a 1.5)

> **Como usar:** Abra cada notebook no Databricks, execute célula por célula, e tire print nos pontos marcados com 📸.
> Os textos explicativos podem ser copiados para o relatório/apresentação.

---

## 1.1 — Conversão para Delta Lake e Transacionalidade

**Notebook:** `tp2_01_delta_lake_conversao`

### Texto explicativo (para o relatório):

A conversão dos dados para o formato Delta Lake transforma o Amazon S3 — que é um storage de objetos simples — em um storage transacional com propriedades ACID. Cada operação de escrita no Delta Lake gera um arquivo JSON numerado sequencialmente dentro do diretório `_delta_log/`. Esse log de transações é o que garante:

- **Atomicidade:** cada commit é tudo-ou-nada — se uma escrita falhar no meio, o commit não é registrado e os dados permanecem consistentes
- **Consistência:** o log valida o schema antes de aceitar novos dados, impedindo inserções com colunas incompatíveis
- **Isolamento:** leitores sempre veem apenas commits completos — nunca dados parciais de uma escrita em andamento
- **Durabilidade:** o log persiste no S3 junto com os arquivos Parquet, garantindo que nenhuma operação seja perdida

Na prática, o `_delta_log` funciona como o "WAL" (Write-Ahead Log) de um banco de dados relacional, mas implementado sobre arquivos no object storage.

### Queries para executar e prints:

**Célula 1 — Conversão dos 9 datasets CSV → Delta:**
```python
# Executar a célula de conversão e capturar a saída com as contagens
# A saída mostrará:
#   ✓ orders: 99.441 registros
#   ✓ customers: 99.441 registros
#   ✓ items: 112.650 registros
#   ... (todos os 9 datasets)
#   ✅ Todos os datasets convertidos para Delta Lake!
```
📸 **PRINT 1:** Saída da conversão mostrando os 9 datasets com contagens

**Célula 2 — Listar arquivos do `_delta_log`:**
```python
delta_log_path = "s3://pb-bronze-brasilmart-234828142988/delta/orders/_delta_log/"
log_files = dbutils.fs.ls(delta_log_path)

print("Arquivos no _delta_log/orders:")
print("-" * 60)
for f in log_files:
    print(f"  {f.name}  ({f.size} bytes)")
```
📸 **PRINT 2:** Lista de arquivos `.json` no `_delta_log` (evidência da existência do log transacional)

**Célula 3 — Ler conteúdo do primeiro commit:**
```python
first_commit = spark.read.json(f"{delta_log_path}00000000000000000000.json")
display(first_commit)
```
📸 **PRINT 3:** Conteúdo do commit JSON mostrando campos `commitInfo`, `add`, `metaData`, `protocol`

**O que cada campo do commit significa** (incluir no relatório):
- **commitInfo**: timestamp da operação, tipo (WRITE), quem executou
- **add**: arquivos Parquet adicionados neste commit (path, tamanho, partições)
- **metaData**: schema da tabela, formato dos dados, configurações
- **protocol**: versão mínima de leitor/escritor para garantir compatibilidade

**Célula 4 — Time Travel (demonstra transacionalidade):**
```python
# Versão 0: dados originais
orders_v0 = spark.read.format("delta").option("versionAsOf", 0).load(f"{BRONZE_PATH}/orders")
print(f"Versão 0 (original): {orders_v0.count()} registros")

# Faz um APPEND simulando nova ingestão
sample = spark.read.format("delta").load(f"{BRONZE_PATH}/orders").limit(100)
sample.write.format("delta").mode("append").save(f"{BRONZE_PATH}/orders")

# Versão 1: com mais registros
orders_v1 = spark.read.format("delta").load(f"{BRONZE_PATH}/orders")
print(f"Versão 1 (após append): {orders_v1.count()} registros")

# Time Travel: voltando para versão 0
orders_v0_again = spark.read.format("delta").option("versionAsOf", 0).load(f"{BRONZE_PATH}/orders")
print(f"Versão 0 (time travel): {orders_v0_again.count()} registros")
```
📸 **PRINT 4:** Saída mostrando as 3 contagens (versão 0, versão 1, time travel de volta)

**Célula 5 — Histórico de operações:**
```python
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, f"{BRONZE_PATH}/orders")
display(dt.history())
```
📸 **PRINT 5:** Tabela de histórico mostrando as operações (WRITE, WRITE append, etc.)

**Célula 6 — Novos arquivos no `_delta_log` após append:**
```python
log_files_after = dbutils.fs.ls(delta_log_path)
print("Arquivos no _delta_log/orders APÓS append:")
print("-" * 60)
for f in log_files_after:
    print(f"  {f.name}  ({f.size} bytes)")
print(f"\nAntes: 1 commit | Depois: {len(log_files_after)} commits")
```
📸 **PRINT 6:** Lista mostrando que agora há 2+ arquivos JSON no `_delta_log` (um por operação)

**Célula 7 — Restaurar versão original:**
```python
dt.restoreToVersion(0)
print(f"✅ Tabela orders restaurada para versão 0: {spark.read.format('delta').load(f'{BRONZE_PATH}/orders').count()} registros")
```
📸 **PRINT 7:** Confirmação da restauração

---

## 1.2 — Unity Catalog: Catálogos, Schemas e Volumes

**Notebook:** `tp2_02_unity_catalog_setup`

### Texto explicativo:

O Unity Catalog é a camada de governança centralizada do Databricks. Ele organiza os dados em uma hierarquia de 3 níveis: Catálogo → Schema → Tabela/Volume. Essa estrutura permite controlar permissões de forma granular — um analista pode ter acesso ao schema `gold` mas não ao `bronze`, por exemplo.

No projeto BrasilMart, criamos:
- **Catálogo `pb_brasilmart`:** agrupa todos os dados do projeto
- **3 Schemas:** `bronze` (dados brutos), `silver` (dados limpos), `gold` (dados analíticos)
- **3 Volumes:** `raw_files` (CSVs originais), `staging_files` (intermediários), `export_files` (exports)

### Queries e prints:

**Célula 1 — Criar catálogo:**
```sql
CREATE CATALOG IF NOT EXISTS pb_brasilmart
COMMENT 'Catálogo do projeto BrasilMart — Visão 360° do Cliente (Olist Dataset)';

USE CATALOG pb_brasilmart;
```
📸 **PRINT 8:** Resultado da criação do catálogo

**Célula 2 — Criar schemas:**
```sql
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Dados brutos convertidos para Delta Lake — sem transformação';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Dados limpos, padronizados e deduplicados';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Tabelas analíticas prontas para consumo (RFM, KPIs, fatos)';
```

**Célula 3 — Verificar schemas:**
```sql
SHOW SCHEMAS IN pb_brasilmart;
```
📸 **PRINT 9:** Lista dos 3 schemas (bronze, silver, gold) + default/information_schema

**Célula 4 — Criar volumes:**
```sql
CREATE VOLUME IF NOT EXISTS bronze.raw_files
COMMENT 'CSVs originais da Olist antes da conversão Delta';

CREATE VOLUME IF NOT EXISTS silver.staging_files
COMMENT 'Arquivos intermediários do processamento Silver';

CREATE VOLUME IF NOT EXISTS gold.export_files
COMMENT 'Exports e relatórios gerados a partir das tabelas Gold';
```

**Célula 5 — Verificar volumes:**
```sql
SHOW VOLUMES IN pb_brasilmart.bronze;
```
📸 **PRINT 10:** Lista de volumes

**Célula 6 — Verificar tabelas registradas:**
```sql
SHOW TABLES IN pb_brasilmart.bronze;
```
📸 **PRINT 11:** Lista das 9 tabelas Bronze no catálogo

**Célula 7 — Detalhes de uma tabela:**
```sql
DESCRIBE EXTENDED pb_brasilmart.bronze.orders;
```
📸 **PRINT 12:** Schema completo da tabela + metadados (Catalog, Database, Table, Type, Location)

📸 **PRINT 13:** No UI do Databricks, navegue até **Catalog → pb_brasilmart** e tire um print mostrando a árvore: catálogo → schemas → tabelas. Isso é a evidência visual mais forte.

### Estrutura final (incluir no relatório):
```
pb_brasilmart (Catálogo)
├── bronze (Schema)
│   ├── orders          (Delta Table — 99.441 registros)
│   ├── customers       (Delta Table — 99.441 registros)
│   ├── items           (Delta Table — 112.650 registros)
│   ├── payments        (Delta Table)
│   ├── reviews         (Delta Table)
│   ├── products        (Delta Table — 32.951 SKUs)
│   ├── sellers         (Delta Table — 3.095 sellers)
│   ├── geolocation     (Delta Table — 1M+ registros)
│   ├── category_translation (Delta Table)
│   └── raw_files       (Volume)
├── silver (Schema)
│   └── staging_files   (Volume)
└── gold (Schema)
    └── export_files    (Volume)
```

---

## 1.3 — Camada Bronze Gerenciada pelo Unity Catalog

**Notebook:** `tp2_03_bronze_unity_catalog`

### Texto explicativo:

A diferença entre uma tabela Delta "avulsa" (External Table) e uma **Managed Table** no Unity Catalog é fundamental:

- **External Table:** os dados ficam em um path S3 que você gerencia manualmente. O Unity Catalog só conhece os metadados.
- **Managed Table:** o Unity Catalog controla tanto os metadados quanto a localização física dos dados. Isso permite governança completa — permissões, auditoria, lineage e lifecycle são gerenciados centralmente.

Cada registro na camada Bronze recebe 3 colunas de rastreabilidade:
- `_ingestao_ts`: timestamp exato da ingestão
- `_fonte`: arquivo CSV de origem
- `_versao_ingestao`: identificador do batch de ingestão

### Queries e prints:

**Célula 1 — Ingestão com metadados (executa para as 9 tabelas):**
```python
# A saída mostra a contagem de cada tabela ingerida:
#   ✓ orders: 99.441 registros
#   ✓ customers: 99.441 registros
#   ... (9 tabelas)
```
📸 **PRINT 14:** Saída completa da ingestão das 9 tabelas com contagens

**Célula 2 — Verificar que são Managed Tables:**
```sql
SHOW TABLES IN pb_brasilmart.bronze;
```
📸 **PRINT 15:** Lista de tabelas

**Célula 3 — Qualidade básica (contagens e nulos):**
```python
# Tabela com contagens e total de nulos por tabela
# Formato da saída:
# Tabela                  Registros   Colunas   Nulos (total)
# orders                     99,441       10           1,234
# customers                  99,441        7               0
# ...
```
📸 **PRINT 16:** Tabela de qualidade mostrando registros, colunas e nulos

**Célula 4 — Provar que é Managed Table:**
```sql
DESCRIBE DETAIL pb_brasilmart.bronze.orders;
```
📸 **PRINT 17:** Resultado mostrando `Type = MANAGED` (coluna importante a destacar)

**Célula 5 — Histórico de operações via Unity Catalog:**
```sql
DESCRIBE HISTORY pb_brasilmart.bronze.orders;
```
📸 **PRINT 18:** Histórico de operações Delta registrado pelo Unity Catalog

---

## 1.4 — Upsert (MERGE) e Time Travel

**Notebook:** `tp2_04_merge_time_travel`

### Texto explicativo:

O MERGE (também chamado de Upsert) é uma operação que combina INSERT e UPDATE em uma única transação atômica. No Delta Lake, a sintaxe é:
- Se o `seller_id` já existe na tabela → **UPDATE** (atualiza os campos)
- Se o `seller_id` não existe → **INSERT** (cria novo registro)

O Time Travel permite acessar qualquer versão anterior da tabela usando `VERSION AS OF` ou `TIMESTAMP AS OF`. Isso é possível porque o `_delta_log` mantém o histórico completo de commits, e os arquivos Parquet antigos são preservados no storage.

### Queries e prints:

**Célula 1 — Estado antes do MERGE:**
```python
df_sellers = spark.table("bronze.sellers")
print(f"Total de sellers: {df_sellers.count()}")
display(df_sellers.limit(5))
```
📸 **PRINT 19:** Contagem original + amostra dos sellers (estado ANTES)

**Célula 2 — Dados preparados para MERGE:**
```python
# Exibe os 4 registros do merge:
# 2 sellers existentes com cidade alterada (UPDATE)
# 2 sellers novos NOVO_SELLER_001 e NOVO_SELLER_002 (INSERT)
display(df_updates)
```
📸 **PRINT 20:** Tabela com os 4 registros — 2 updates + 2 inserts

**Célula 3 — Executar o MERGE:**
```python
dt_sellers = DeltaTable.forName(spark, "pb_brasilmart.bronze.sellers")

(dt_sellers.alias("target")
 .merge(
     df_updates.alias("source"),
     "target.seller_id = source.seller_id"
 )
 .whenMatchedUpdate(set={
     "seller_zip_code_prefix": "source.seller_zip_code_prefix",
     "seller_city":            "source.seller_city",
     "seller_state":           "source.seller_state",
     "_ingestao_ts":           "source._ingestao_ts",
     "_fonte":                 "source._fonte",
     "_versao_ingestao":       "source._versao_ingestao",
 })
 .whenNotMatchedInsertAll()
 .execute()
)
print("✅ MERGE executado!")
```
📸 **PRINT 21:** Confirmação do MERGE executado

**Célula 4 — Verificar resultado:**
```python
# Mostra: Antes do MERGE: 3.095 sellers
#         Depois do MERGE: 3.097 sellers
#         Novos inseridos: 2
```
📸 **PRINT 22:** Comparação antes/depois mostrando +2 sellers

**Célula 5 — Verificar sellers atualizados e novos:**
```python
# Sellers ATUALIZADOS (UPDATE):
display(df_after.where(F.col("seller_id").isin(id1, id2)))

# Sellers NOVOS (INSERT):
display(df_after.where(F.col("seller_id").startswith("NOVO_SELLER")))
```
📸 **PRINT 23:** Tabelas mostrando sellers atualizados (cidade mudou) e novos inseridos

**Célula 6 — Histórico de versões:**
```sql
DESCRIBE HISTORY pb_brasilmart.bronze.sellers;
```
📸 **PRINT 24:** Histórico mostrando operações WRITE → MERGE

**Célula 7 — Time Travel — comparar versões:**
```python
df_antes = spark.read.format("delta").option("versionAsOf", versao_antes_merge).table("pb_brasilmart.bronze.sellers")
df_depois = spark.table("bronze.sellers")

print(f"Versão {versao_antes_merge} (antes): {df_antes.count()} sellers")
print(f"Versão {versao_merge} (depois):  {df_depois.count()} sellers")
```
📸 **PRINT 25:** Contagens comparativas entre versões

**Célula 8 — Seller antes e depois do MERGE:**
```python
print(f"Seller {id1} ANTES do merge:")
display(df_antes.where(F.col("seller_id") == id1))

print(f"Seller {id1} DEPOIS do merge:")
display(df_depois.where(F.col("seller_id") == id1))
```
📸 **PRINT 26:** Comparação lado a lado do mesmo seller (cidade mudou)

**Célula 9 — Sellers novos não existiam na versão anterior:**
```python
# Deve retornar VAZIO — prova que não existiam antes
display(df_antes.where(F.col("seller_id").startswith("NOVO_SELLER")))
```
📸 **PRINT 27:** Resultado vazio — prova que NOVO_SELLER não existia na versão anterior

**Célula 10 — SQL Time Travel:**
```sql
SELECT seller_id, seller_city, seller_state, _versao_ingestao
FROM pb_brasilmart.bronze.sellers VERSION AS OF 0
WHERE seller_id IN (SELECT seller_id FROM pb_brasilmart.bronze.sellers WHERE seller_id LIKE 'NOVO_SELLER%')
   OR seller_city IN ('São Paulo', 'Rio de Janeiro');
```
📸 **PRINT 28:** Resultado do Time Travel via SQL

**Célula 11 — RESTORE (restaurar versão anterior):**
```python
# Antes do RESTORE: 3.097 sellers
spark.sql(f"RESTORE TABLE pb_brasilmart.bronze.sellers TO VERSION AS OF {versao_antes_merge}")
# Depois do RESTORE: 3.095 sellers
```
📸 **PRINT 29:** Saída mostrando restauração bem-sucedida (contagem voltou ao original)

**Célula 12 — Histórico final com RESTORE:**
```sql
DESCRIBE HISTORY pb_brasilmart.bronze.sellers;
```
📸 **PRINT 30:** Histórico completo: WRITE → MERGE → RESTORE

---

## 1.5 — Otimização com OPTIMIZE e Z-ORDER

**Notebook:** `tp2_05_optimize_zorder`

### Texto explicativo:

O Delta Lake acumula muitos arquivos pequenos (small files problem) após múltiplas operações de escrita. Isso degrada a performance porque o Spark precisa abrir e ler cada arquivo individualmente.

- **OPTIMIZE:** compacta muitos arquivos pequenos em menos arquivos maiores (~1GB), reduzindo o overhead de I/O
- **Z-ORDER:** reorganiza fisicamente os dados dentro dos arquivos pela coluna especificada. Quando uma consulta filtra por essa coluna (`WHERE customer_id = X`), o Spark consegue pular arquivos inteiros que não contêm o valor procurado (data skipping)
- **VACUUM:** remove arquivos Parquet obsoletos (que não são mais referenciados por nenhuma versão ativa), economizando custo de storage

A escolha das colunas Z-ORDER foi baseada nos filtros mais comuns do negócio:
- `orders` → `customer_id, order_status` (visão 360° do cliente e análise por status)
- `items` → `order_id, product_id` (detalhamento de pedidos e análise de produtos)
- `payments` → `order_id` (pagamentos de um pedido específico)
- `geolocation` → `geolocation_zip_code_prefix` (análise regional)

### Queries e prints:

**Célula 1 — Diagnóstico ANTES da otimização:**
```sql
DESCRIBE DETAIL pb_brasilmart.bronze.orders;
```
📸 **PRINT 31:** Detalhes da tabela mostrando `numFiles` e `sizeInBytes` ANTES

**Célula 2 — Diagnóstico de todas as tabelas:**
```python
# Formato da saída:
# Tabela                    Arquivos   Tamanho (MB)
# orders                          3           7.42
# customers                       1           3.81
# ...
```
📸 **PRINT 32:** Tabela com arquivos e tamanho de TODAS as tabelas Bronze (ANTES)

**Célula 3 — OPTIMIZE (compactação):**
```sql
OPTIMIZE pb_brasilmart.bronze.orders;
OPTIMIZE pb_brasilmart.bronze.items;
OPTIMIZE pb_brasilmart.bronze.payments;
OPTIMIZE pb_brasilmart.bronze.customers;
OPTIMIZE pb_brasilmart.bronze.geolocation;
```
📸 **PRINT 33:** Resultado do OPTIMIZE mostrando métricas (numFilesAdded, numFilesRemoved)

**Célula 4 — Z-ORDER:**
```sql
OPTIMIZE pb_brasilmart.bronze.orders
ZORDER BY (customer_id, order_status);

OPTIMIZE pb_brasilmart.bronze.items
ZORDER BY (order_id, product_id);

OPTIMIZE pb_brasilmart.bronze.payments
ZORDER BY (order_id);

OPTIMIZE pb_brasilmart.bronze.geolocation
ZORDER BY (geolocation_zip_code_prefix);
```
📸 **PRINT 34:** Resultado do Z-ORDER (pelo menos um dos comandos)

**Célula 5 — Diagnóstico DEPOIS:**
```python
# Mesma tabela, agora com os números pós-otimização
# Tabela                    Arquivos   Tamanho (MB)
# orders                          1           6.95
# ...
```
📸 **PRINT 35:** Tabela com arquivos e tamanho DEPOIS (comparar com PRINT 32)

**Célula 6 — Benchmark de performance:**
```python
# Saída esperada:
# Consulta: WHERE order_status = 'delivered'
#   Versão anterior: 96.478 registros em 2.34s
#   Versão otimizada: 96.478 registros em 0.87s
```
📸 **PRINT 36:** Comparação de tempo antes/depois do Z-ORDER

**Célula 7 — Histórico de operações:**
```sql
DESCRIBE HISTORY pb_brasilmart.bronze.orders;
```
📸 **PRINT 37:** Histórico mostrando operações OPTIMIZE registradas

**Célula 8 — VACUUM Dry Run:**
```sql
VACUUM pb_brasilmart.bronze.orders DRY RUN;
```
📸 **PRINT 38:** Resultado do VACUUM dry run mostrando arquivos que seriam removidos

---

## Resumo de Prints

| Print | Item | O que capturar |
|-------|------|---------------|
| 1 | 1.1 | Conversão 9 datasets CSV → Delta com contagens |
| 2 | 1.1 | Arquivos no `_delta_log/` |
| 3 | 1.1 | Conteúdo JSON do primeiro commit |
| 4 | 1.1 | Time Travel: 3 contagens (v0, v1, time travel) |
| 5 | 1.1 | Histórico de operações Delta |
| 6 | 1.1 | `_delta_log` com múltiplos commits |
| 7 | 1.1 | Restauração para versão original |
| 8 | 1.2 | Criação do catálogo `pb_brasilmart` |
| 9 | 1.2 | Lista de schemas (bronze, silver, gold) |
| 10 | 1.2 | Lista de volumes |
| 11 | 1.2 | Lista de tabelas no schema bronze |
| 12 | 1.2 | DESCRIBE EXTENDED de uma tabela |
| 13 | 1.2 | UI do Catalog Explorer (árvore visual) |
| 14 | 1.3 | Ingestão das 9 tabelas como Managed Tables |
| 15 | 1.3 | Lista de tabelas Bronze |
| 16 | 1.3 | Tabela de qualidade (contagens + nulos) |
| 17 | 1.3 | DESCRIBE DETAIL mostrando Type = MANAGED |
| 18 | 1.3 | DESCRIBE HISTORY via Unity Catalog |
| 19 | 1.4 | Estado ANTES do MERGE (contagem + amostra) |
| 20 | 1.4 | Dados preparados para MERGE (4 registros) |
| 21 | 1.4 | Confirmação do MERGE executado |
| 22 | 1.4 | Comparação antes/depois (+2 sellers) |
| 23 | 1.4 | Sellers atualizados + novos |
| 24 | 1.4 | DESCRIBE HISTORY com operação MERGE |
| 25 | 1.4 | Time Travel: contagens por versão |
| 26 | 1.4 | Mesmo seller antes/depois (cidade mudou) |
| 27 | 1.4 | Sellers novos não existiam antes (vazio) |
| 28 | 1.4 | SQL VERSION AS OF |
| 29 | 1.4 | RESTORE bem-sucedido |
| 30 | 1.4 | Histórico: WRITE → MERGE → RESTORE |
| 31 | 1.5 | DESCRIBE DETAIL antes do OPTIMIZE |
| 32 | 1.5 | Diagnóstico de todas as tabelas (ANTES) |
| 33 | 1.5 | Resultado do OPTIMIZE (métricas) |
| 34 | 1.5 | Resultado do Z-ORDER |
| 35 | 1.5 | Diagnóstico de todas as tabelas (DEPOIS) |
| 36 | 1.5 | Benchmark de performance (antes vs depois) |
| 37 | 1.5 | Histórico com operações OPTIMIZE |
| 38 | 1.5 | VACUUM DRY RUN |

**Total: 38 prints para os itens 1.1 a 1.5**
