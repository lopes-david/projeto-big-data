# Databricks notebook source
# MAGIC %md
# MAGIC # TP4 — Atividade 2.2: Permissões Finas (Column-Level Security)
# MAGIC
# MAGIC **Objetivo**: Usar Lake Formation para restringir o acesso a colunas PII
# MAGIC para o role "Analista Jr." no AWS Athena.
# MAGIC
# MAGIC ## Arquitetura de Segurança
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                    AWS Lake Formation                          │
# MAGIC │                  (Column-Level Security)                       │
# MAGIC ├─────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                │
# MAGIC │  Admin (root)          Analista Jr.                            │
# MAGIC │  ┌──────────────┐      ┌──────────────────────────────────┐   │
# MAGIC │  │ ALL columns  │      │ SELECT * EXCEPT PII columns     │   │
# MAGIC │  │ ALL tables   │      │                                  │   │
# MAGIC │  │ ALL databases│      │ customers: ✗ customer_id         │   │
# MAGIC │  └──────────────┘      │            ✗ customer_unique_id  │   │
# MAGIC │                        │ sellers:   ✗ seller_id           │   │
# MAGIC │                        │ geoloc:    ✗ lat, lng            │   │
# MAGIC │                        │ reviews:   ✗ comment_title       │   │
# MAGIC │                        │            ✗ comment_message     │   │
# MAGIC │                        │ orders:    ✓ TODAS               │   │
# MAGIC │                        │ Gold:      ✓ TODAS (agregadas)   │   │
# MAGIC │                        └──────────────────────────────────┘   │
# MAGIC │                                                                │
# MAGIC │  Mecanismo: ColumnWildcard + ExcludedColumnNames               │
# MAGIC │  Athena → Glue Data Catalog → Lake Formation → S3             │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração Realizada
# MAGIC
# MAGIC ### IAM Role criada
# MAGIC - **Nome**: `pb-brasilmart-analista-jr`
# MAGIC - **ARN**: `arn:aws:iam::234828142988:role/pb-brasilmart-analista-jr`
# MAGIC - **Políticas**: Athena (query), Glue (read-only), S3 (silver+gold), Lake Formation (GetDataAccess)
# MAGIC
# MAGIC ### Column-Level Permissions (Lake Formation)
# MAGIC
# MAGIC | Tabela | Database | Colunas BLOQUEADAS | Motivo |
# MAGIC |--------|----------|--------------------|--------|
# MAGIC | customers | bronze, silver | `customer_id`, `customer_unique_id` | Hash CPF — LGPD Art.5 |
# MAGIC | sellers | bronze, silver | `seller_id` | Hash CNPJ/CPF |
# MAGIC | geolocation | bronze | `geolocation_lat`, `geolocation_lng` | Coordenadas GPS — endereço exato |
# MAGIC | reviews | bronze | `review_comment_title`, `review_comment_message` | Texto livre — pode conter PII |
# MAGIC | orders | bronze | *(nenhuma)* | Sem PII direto |
# MAGIC | items | bronze | *(nenhuma)* | Sem PII direto |
# MAGIC | payments | bronze | *(nenhuma)* | Sem PII direto |
# MAGIC | products | bronze | *(nenhuma)* | Sem PII direto |
# MAGIC | Gold (todas) | gold | *(nenhuma)* | Dados agregados |
# MAGIC
# MAGIC ### Mecanismo Lake Formation
# MAGIC ```json
# MAGIC {
# MAGIC   "TableWithColumns": {
# MAGIC     "DatabaseName": "pb_bronze_brasilmart",
# MAGIC     "Name": "customers",
# MAGIC     "ColumnWildcard": {
# MAGIC       "ExcludedColumnNames": ["customer_id", "customer_unique_id"]
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC O `ColumnWildcard` com `ExcludedColumnNames` concede SELECT em **todas as colunas EXCETO** as listadas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Teste de Validação no Athena

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query que FUNCIONA (Analista Jr.) — colunas não-PII
# MAGIC ```sql
# MAGIC -- Athena (assumindo role pb-brasilmart-analista-jr)
# MAGIC SELECT customer_zip_code_prefix,
# MAGIC        customer_city,
# MAGIC        customer_state
# MAGIC FROM pb_bronze_brasilmart.customers
# MAGIC LIMIT 10;
# MAGIC ```
# MAGIC **Resultado**: ✅ Retorna dados normalmente (3 colunas visíveis)
# MAGIC
# MAGIC ### Query que FALHA (Analista Jr.) — coluna PII bloqueada
# MAGIC ```sql
# MAGIC -- Athena (assumindo role pb-brasilmart-analista-jr)
# MAGIC SELECT customer_id, customer_city
# MAGIC FROM pb_bronze_brasilmart.customers
# MAGIC LIMIT 10;
# MAGIC ```
# MAGIC **Resultado**: ❌ `AccessDeniedException: Insufficient Lake Formation permission(s) on customers`
# MAGIC
# MAGIC ### Query que FALHA (Analista Jr.) — SELECT * inclui PII
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM pb_bronze_brasilmart.customers
# MAGIC LIMIT 10;
# MAGIC ```
# MAGIC **Resultado**: ❌ `AccessDeniedException` (SELECT * tenta acessar customer_id)
# MAGIC
# MAGIC ### Query Gold que FUNCIONA (dados agregados)
# MAGIC ```sql
# MAGIC SELECT rfm_segment, COUNT(*) as total
# MAGIC FROM pb_gold_brasilmart.dim_clientes_rfm
# MAGIC GROUP BY rfm_segment;
# MAGIC ```
# MAGIC **Resultado**: ✅ Analista Jr. tem acesso completo ao Gold (dados agregados, sem PII individual)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Simulação no Databricks (Unity Catalog)

# COMMAND ----------

catalog = "pb_brasilmart"

print("=" * 70)
print("Simulacao: visao do Analista Jr. nas tabelas Bronze")
print("=" * 70)

analista_jr_blocked = {
    "bronze.customers": ["customer_id", "customer_unique_id"],
    "bronze.sellers": ["seller_id"],
    "bronze.geolocation": ["geolocation_lat", "geolocation_lng"],
    "bronze.reviews": ["review_comment_title", "review_comment_message"],
}

for table_ref, blocked_cols in analista_jr_blocked.items():
    schema, table = table_ref.split(".")
    full_name = f"{catalog}.{schema}.{table}"

    try:
        all_cols = [c["col_name"] for c in spark.sql(f"DESCRIBE TABLE {full_name}").collect()
                    if c["data_type"] != "" and not c["col_name"].startswith("#")]
    except Exception as e:
        print(f"\n  SKIP {full_name}: {str(e)[:60]}")
        continue

    visible_cols = [c for c in all_cols if c not in blocked_cols]

    print(f"\n--- {full_name} ---")
    print(f"  Total colunas:     {len(all_cols)}")
    print(f"  Visiveis (Jr.):    {len(visible_cols)} → {visible_cols}")
    print(f"  Bloqueadas (PII):  {len(blocked_cols)} → {blocked_cols}")

    if visible_cols:
        cols_sql = ", ".join(visible_cols)
        print(f"  Query permitida:   SELECT {cols_sql} FROM {full_name} LIMIT 5")
        df = spark.sql(f"SELECT {cols_sql} FROM {full_name} LIMIT 5")
        df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificação de Permissões via AWS CLI
# MAGIC
# MAGIC ```bash
# MAGIC # Listar permissoes do Analista Jr. no Lake Formation
# MAGIC aws lakeformation list-permissions \
# MAGIC   --principal '{"DataLakePrincipalIdentifier": "arn:aws:iam::234828142988:role/pb-brasilmart-analista-jr"}' \
# MAGIC   --region sa-east-1 \
# MAGIC   --query 'PrincipalResourcePermissions[*].{
# MAGIC     Resource: Resource,
# MAGIC     Permissions: Permissions
# MAGIC   }' \
# MAGIC   --output table
# MAGIC ```
# MAGIC
# MAGIC ### Resultado esperado:
# MAGIC ```
# MAGIC | Resource (TableWithColumns)        | ExcludedColumns              | Permissions |
# MAGIC |------------------------------------|------------------------------|-------------|
# MAGIC | bronze.customers                   | customer_id, unique_id       | SELECT      |
# MAGIC | bronze.sellers                     | seller_id                    | SELECT      |
# MAGIC | bronze.geolocation                 | geolocation_lat, lng         | SELECT      |
# MAGIC | bronze.reviews                     | comment_title, message       | SELECT      |
# MAGIC | bronze.orders                      | (nenhuma)                    | SELECT      |
# MAGIC | gold.*                             | (nenhuma)                    | SELECT,DESC |
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumo — Matriz de Acesso
# MAGIC
# MAGIC | Persona | Bronze | Silver | Gold | PII Columns | Athena | Databricks |
# MAGIC |---------|--------|--------|------|-------------|--------|------------|
# MAGIC | **Admin** | Full | Full | Full | ✅ Visível | ✅ | ✅ |
# MAGIC | **Analista Jr.** | SELECT (sem PII) | SELECT (sem PII) | Full | ❌ Bloqueado | ✅ (filtrado) | ❌ |
# MAGIC | **Glue ETL** | Full | Full | Full | ✅ (processamento) | N/A | N/A |
# MAGIC
# MAGIC ### Arquivos de configuração:
# MAGIC - Script AWS CLI: `infra/aws/setup_column_security.sh`
# MAGIC - Terraform: `infra/terraform/column_security.tf`
