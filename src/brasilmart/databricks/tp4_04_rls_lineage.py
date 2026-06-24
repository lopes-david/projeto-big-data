# Databricks notebook source
# MAGIC %md
# MAGIC # TP4 — Atividade 2.3: Row-Level Security + Linhagem de Dados
# MAGIC
# MAGIC ## Parte 1: Row-Level Security (RLS) no Unity Catalog
# MAGIC Controle de acesso em nível de linha na tabela Silver para que o grupo
# MAGIC `Regiao_Norte` veja apenas dados de clientes/vendedores da região Norte.
# MAGIC
# MAGIC ## Parte 2: Linhagem de Dados (Data Lineage)
# MAGIC Rastreamento da coluna `tempo_total_seg` (velocidade de entrega) e
# MAGIC `total_item_value` desde a Camada Bronze até a Camada Gold.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Parte 1 — Row-Level Security (RLS)
# MAGIC
# MAGIC ## Arquitetura
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────┐
# MAGIC │         Unity Catalog — Row-Level Security           │
# MAGIC ├──────────────────────────────────────────────────────┤
# MAGIC │                                                      │
# MAGIC │  Grupo: Regiao_Norte                                 │
# MAGIC │  ┌────────────────────────────────────────────────┐  │
# MAGIC │  │ SQL Function: filtro_regiao_norte()            │  │
# MAGIC │  │   IF IS_ACCOUNT_GROUP_MEMBER('Regiao_Norte')   │  │
# MAGIC │  │     → WHERE customer_state IN (AM,PA,AC,...)   │  │
# MAGIC │  │   ELSE → sem filtro (admin vê tudo)            │  │
# MAGIC │  └────────────────────────────────────────────────┘  │
# MAGIC │                                                      │
# MAGIC │  ALTER TABLE silver.orders_enriched                  │
# MAGIC │    SET ROW FILTER filtro_regiao_norte ON (state)     │
# MAGIC │                                                      │
# MAGIC │  Resultado:                                          │
# MAGIC │    Admin     → SELECT * → 99.441 rows (todas)       │
# MAGIC │    Reg.Norte → SELECT * → ~X rows (AM,PA,AC,...)    │
# MAGIC └──────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Estados da Região Norte do Brasil
# MAGIC | UF | Estado |
# MAGIC |----|--------|
# MAGIC | AM | Amazonas |
# MAGIC | PA | Pará |
# MAGIC | AC | Acre |
# MAGIC | RO | Rondônia |
# MAGIC | RR | Roraima |
# MAGIC | AP | Amapá |
# MAGIC | TO | Tocantins |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Criar o grupo Regiao_Norte no Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar se o grupo ja existe (criado via SCIM ou Account Console)
# MAGIC -- No Account Console: Settings → Identity and access → Groups → Add group
# MAGIC -- Nome: Regiao_Norte
# MAGIC -- Membros: adicionar usuarios que devem ter acesso restrito à região Norte

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Criar a função SQL de filtro RLS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Funcao de filtro: retorna TRUE para as linhas que o usuario pode ver
# MAGIC -- Se o usuario pertence ao grupo Regiao_Norte, so ve dados da regiao Norte
# MAGIC -- Se NAO pertence (ex: admin), ve todas as linhas
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION pb_brasilmart.silver.filtro_regiao_norte(customer_state_param STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'TP4 RLS: filtra linhas para que o grupo Regiao_Norte veja apenas dados da regiao Norte (AM,PA,AC,RO,RR,AP,TO)'
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('Regiao_Norte')
# MAGIC     THEN customer_state_param IN ('AM', 'PA', 'AC', 'RO', 'RR', 'AP', 'TO')
# MAGIC     ELSE TRUE
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Aplicar Row Filter na tabela Silver orders_enriched

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aplicar o filtro RLS na tabela orders_enriched
# MAGIC -- A coluna customer_state sera passada como argumento para a funcao
# MAGIC ALTER TABLE pb_brasilmart.silver.orders_enriched
# MAGIC SET ROW FILTER pb_brasilmart.silver.filtro_regiao_norte ON (customer_state);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Aplicar Row Filter na tabela Silver customers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Criar funcao de filtro para a tabela customers (mesma logica, coluna diferente)
# MAGIC CREATE OR REPLACE FUNCTION pb_brasilmart.silver.filtro_regiao_norte_customers(state_param STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'TP4 RLS: filtra clientes para que Regiao_Norte veja apenas clientes do Norte'
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('Regiao_Norte')
# MAGIC     THEN state_param IN ('AM', 'PA', 'AC', 'RO', 'RR', 'AP', 'TO')
# MAGIC     ELSE TRUE
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE pb_brasilmart.silver.customers
# MAGIC SET ROW FILTER pb_brasilmart.silver.filtro_regiao_norte_customers ON (customer_state);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Aplicar Row Filter na tabela Silver sellers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION pb_brasilmart.silver.filtro_regiao_norte_sellers(state_param STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'TP4 RLS: filtra vendedores para que Regiao_Norte veja apenas sellers do Norte'
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('Regiao_Norte')
# MAGIC     THEN state_param IN ('AM', 'PA', 'AC', 'RO', 'RR', 'AP', 'TO')
# MAGIC     ELSE TRUE
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE pb_brasilmart.silver.sellers
# MAGIC SET ROW FILTER pb_brasilmart.silver.filtro_regiao_norte_sellers ON (seller_state);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 Verificar os filtros aplicados

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Listar todas as tabelas com Row Filter ativo
# MAGIC SELECT
# MAGIC   table_catalog,
# MAGIC   table_schema,
# MAGIC   table_name,
# MAGIC   row_filter
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_catalog = 'pb_brasilmart'
# MAGIC   AND row_filter IS NOT NULL
# MAGIC ORDER BY table_schema, table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.7 Teste — Visão do Admin (sem filtro)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Admin ve TODAS as linhas (nao pertence ao grupo Regiao_Norte)
# MAGIC SELECT
# MAGIC   customer_state,
# MAGIC   COUNT(*) AS total_pedidos
# MAGIC FROM pb_brasilmart.silver.orders_enriched
# MAGIC GROUP BY customer_state
# MAGIC ORDER BY total_pedidos DESC;

# COMMAND ----------

# Contar total para comparacao
total_admin = spark.sql("SELECT COUNT(*) AS total FROM pb_brasilmart.silver.orders_enriched").first()["total"]
print(f"Visao ADMIN: {total_admin} pedidos (todas as regioes)")

norte_count = spark.sql("""
    SELECT COUNT(*) AS total
    FROM pb_brasilmart.silver.orders_enriched
    WHERE customer_state IN ('AM', 'PA', 'AC', 'RO', 'RR', 'AP', 'TO')
""").first()["total"]
print(f"Visao REGIAO_NORTE (simulada): {norte_count} pedidos (apenas Norte)")
print(f"Percentual filtrado: {norte_count/total_admin*100:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.8 Teste — Simulação da visão Regiao_Norte

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simulacao: o que o usuario Regiao_Norte veria
# MAGIC -- (executar como membro do grupo para ver o filtro real em acao)
# MAGIC SELECT
# MAGIC   customer_state,
# MAGIC   COUNT(*) AS total_pedidos,
# MAGIC   ROUND(AVG(total_pago), 2) AS ticket_medio,
# MAGIC   ROUND(AVG(tempo_total_seg) / 86400, 1) AS tempo_medio_entrega_dias
# MAGIC FROM pb_brasilmart.silver.orders_enriched
# MAGIC WHERE customer_state IN ('AM', 'PA', 'AC', 'RO', 'RR', 'AP', 'TO')
# MAGIC GROUP BY customer_state
# MAGIC ORDER BY total_pedidos DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.9 Conceder permissões ao grupo Regiao_Norte

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Permissao de leitura nas tabelas Silver (com RLS ativo)
# MAGIC GRANT USE CATALOG ON CATALOG pb_brasilmart TO `Regiao_Norte`;
# MAGIC GRANT USE SCHEMA ON SCHEMA pb_brasilmart.silver TO `Regiao_Norte`;
# MAGIC GRANT SELECT ON TABLE pb_brasilmart.silver.orders_enriched TO `Regiao_Norte`;
# MAGIC GRANT SELECT ON TABLE pb_brasilmart.silver.customers TO `Regiao_Norte`;
# MAGIC GRANT SELECT ON TABLE pb_brasilmart.silver.sellers TO `Regiao_Norte`;
# MAGIC
# MAGIC -- Permissao de EXECUTE na funcao de filtro (necessario para o RLS funcionar)
# MAGIC GRANT EXECUTE ON FUNCTION pb_brasilmart.silver.filtro_regiao_norte TO `Regiao_Norte`;
# MAGIC GRANT EXECUTE ON FUNCTION pb_brasilmart.silver.filtro_regiao_norte_customers TO `Regiao_Norte`;
# MAGIC GRANT EXECUTE ON FUNCTION pb_brasilmart.silver.filtro_regiao_norte_sellers TO `Regiao_Norte`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resumo RLS
# MAGIC
# MAGIC | Tabela | Função de Filtro | Coluna Filtrada | Comportamento |
# MAGIC |--------|------------------|-----------------|---------------|
# MAGIC | `silver.orders_enriched` | `filtro_regiao_norte` | `customer_state` | Norte: AM,PA,AC,RO,RR,AP,TO |
# MAGIC | `silver.customers` | `filtro_regiao_norte_customers` | `customer_state` | Idem |
# MAGIC | `silver.sellers` | `filtro_regiao_norte_sellers` | `seller_state` | Idem |
# MAGIC
# MAGIC **Mecanismo**: `IS_ACCOUNT_GROUP_MEMBER('Regiao_Norte')` — se TRUE, filtra; se FALSE (admin), mostra tudo.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Parte 2 — Linhagem de Dados (Data Lineage)
# MAGIC
# MAGIC ## 2.1 Coluna Rastreada: `tempo_total_seg` (Velocidade de Entrega)
# MAGIC
# MAGIC Representa o **tempo total em segundos** do ciclo de vida do pedido
# MAGIC (compra → entrega), definido como requisito de negócio no TP1
# MAGIC (velocidade logística / SLA de entrega).
# MAGIC
# MAGIC ### Linhagem completa:
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────────────┐
# MAGIC │                    LINEAGE: tempo_total_seg                              │
# MAGIC ├──────────────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                          │
# MAGIC │  BRONZE (raw)                                                            │
# MAGIC │  └─ pb_brasilmart.bronze.orders                                          │
# MAGIC │     ├─ order_purchase_timestamp (STRING, raw)                            │
# MAGIC │     └─ order_delivered_customer_date (STRING, raw)                       │
# MAGIC │                          │                                               │
# MAGIC │                          ▼ CAST + CALCULO                                │
# MAGIC │  SILVER (DLT pipeline: tp3_01_dlt_silver.py)                             │
# MAGIC │  └─ pb_brasilmart.silver.orders                                          │
# MAGIC │     └─ tempo_total_seg = unix_timestamp(delivered) -                     │
# MAGIC │                          unix_timestamp(purchase)                        │
# MAGIC │                          │                                               │
# MAGIC │                          ▼ PASSTHROUGH (dlt.read join)                   │
# MAGIC │  SILVER ENRICHED (DLT: orders + customers + payments)                    │
# MAGIC │  └─ pb_brasilmart.silver.orders_enriched                                 │
# MAGIC │     └─ tempo_total_seg (mantido do join com silver.orders)               │
# MAGIC │                          │                                               │
# MAGIC │                          ▼ EXPORT (Spark-Redshift connector)             │
# MAGIC │  REDSHIFT (raw_databricks.orders)                                        │
# MAGIC │  └─ dev.raw_databricks.orders                                            │
# MAGIC │     └─ tempo_total_seg (BIGINT)                                          │
# MAGIC │                          │                                               │
# MAGIC │                          ▼ VIEW (dbt staging)                            │
# MAGIC │  REDSHIFT STAGING (dbt: stg_orders)                                      │
# MAGIC │  └─ dev.pb_silver.stg_orders                                             │
# MAGIC │     └─ tempo_total_seg (passthrough)                                     │
# MAGIC │                                                                          │
# MAGIC └──────────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1.1 Evidência — Bronze (dados brutos)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BRONZE: colunas de origem (strings brutas)
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   order_purchase_timestamp,
# MAGIC   order_delivered_customer_date,
# MAGIC   typeof(order_purchase_timestamp) AS tipo_purchase,
# MAGIC   typeof(order_delivered_customer_date) AS tipo_delivered
# MAGIC FROM pb_brasilmart.bronze.orders
# MAGIC WHERE order_delivered_customer_date IS NOT NULL
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1.2 Evidência — Silver (campo calculado)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SILVER: tempo_total_seg calculado = unix_timestamp(delivered) - unix_timestamp(purchase)
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   order_purchase_timestamp,
# MAGIC   order_delivered_customer_date,
# MAGIC   tempo_total_seg,
# MAGIC   ROUND(tempo_total_seg / 86400.0, 1) AS tempo_total_dias,
# MAGIC   status_entrega,
# MAGIC   typeof(tempo_total_seg) AS tipo_coluna
# MAGIC FROM pb_brasilmart.silver.orders
# MAGIC WHERE tempo_total_seg IS NOT NULL
# MAGIC ORDER BY tempo_total_seg DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1.3 Evidência — Silver Enriched (passthrough + contexto)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SILVER ENRICHED: tempo_total_seg mantido do join, com contexto de cliente e pagamento
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   customer_state,
# MAGIC   customer_city,
# MAGIC   tempo_total_seg,
# MAGIC   ROUND(tempo_total_seg / 86400.0, 1) AS tempo_total_dias,
# MAGIC   status_entrega,
# MAGIC   total_pago,
# MAGIC   metodo_pagamento_principal
# MAGIC FROM pb_brasilmart.silver.orders_enriched
# MAGIC WHERE tempo_total_seg IS NOT NULL
# MAGIC   AND customer_state IN ('AM', 'PA', 'SP')
# MAGIC ORDER BY tempo_total_seg DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Coluna Rastreada: `total_item_value` → Gold `gmv` (Receita)
# MAGIC
# MAGIC Rastreamento end-to-end de Bronze até Gold (agregação final).
# MAGIC
# MAGIC ### Linhagem completa:
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────────────┐
# MAGIC │              LINEAGE: price + freight → total_item_value → gmv           │
# MAGIC ├──────────────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                          │
# MAGIC │  BRONZE                                                                  │
# MAGIC │  └─ pb_brasilmart.bronze.items                                           │
# MAGIC │     ├─ price (STRING, raw)                                               │
# MAGIC │     └─ freight_value (STRING, raw)                                       │
# MAGIC │                          │                                               │
# MAGIC │                          ▼ CAST + SOMA                                   │
# MAGIC │  SILVER (DLT: tp3_01_dlt_silver.py → silver_items)                       │
# MAGIC │  └─ pb_brasilmart.silver.items                                           │
# MAGIC │     └─ total_item_value = ROUND(price + freight_value, 2)                │
# MAGIC │                          │                                               │
# MAGIC │                          ▼ EXPORT → dbt staging                          │
# MAGIC │  REDSHIFT STAGING (dbt: stg_items)                                       │
# MAGIC │  └─ dev.pb_silver.stg_items                                              │
# MAGIC │     └─ total_item_value (passthrough)                                    │
# MAGIC │                          │                                               │
# MAGIC │                          ▼ AGREGAÇÃO (dbt mart)                          │
# MAGIC │  GOLD (dbt: fato_vendas_diarias)                                         │
# MAGIC │  └─ dev.pb_gold.fato_vendas_diarias                                     │
# MAGIC │     ├─ gmv = SUM(total_item_value)         ← receita bruta diária       │
# MAGIC │     └─ ticket_medio = AVG(total_item_value) ← ticket médio diário       │
# MAGIC │                          │                                               │
# MAGIC │                          ▼ AGREGAÇÃO (dbt mart)                          │
# MAGIC │  GOLD (dbt: dim_clientes_rfm)                                           │
# MAGIC │  └─ dev.pb_gold.dim_clientes_rfm                                        │
# MAGIC │     └─ monetary = SUM(order_total)                                       │
# MAGIC │        onde order_total = SUM(total_item_value) por pedido               │
# MAGIC │                                                                          │
# MAGIC └──────────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2.1 Evidência — Bronze (valores brutos)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BRONZE: price e freight_value como vieram do CSV
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   order_item_id,
# MAGIC   price,
# MAGIC   freight_value,
# MAGIC   typeof(price) AS tipo_price,
# MAGIC   typeof(freight_value) AS tipo_freight
# MAGIC FROM pb_brasilmart.bronze.items
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2.2 Evidência — Silver (campo calculado)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SILVER: total_item_value = ROUND(CAST(price) + CAST(freight_value), 2)
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   order_item_id,
# MAGIC   price,
# MAGIC   freight_value,
# MAGIC   total_item_value,
# MAGIC   (price + freight_value) AS soma_manual,
# MAGIC   (total_item_value = ROUND(price + freight_value, 2)) AS calculo_correto
# MAGIC FROM pb_brasilmart.silver.items
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2.3 Evidência — Gold (agregação final)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Demonstrar a linhagem: Silver items → Gold fato_vendas_diarias
# MAGIC -- total_item_value → SUM → gmv (por dia)

# MAGIC -- Recalcular um dia especifico para provar a linhagem
# MAGIC WITH silver_day AS (
# MAGIC   SELECT
# MAGIC     DATE(o.order_purchase_timestamp) AS data_venda,
# MAGIC     SUM(i.total_item_value) AS gmv_recalculado,
# MAGIC     AVG(i.total_item_value) AS ticket_recalculado
# MAGIC   FROM pb_brasilmart.silver.orders o
# MAGIC   JOIN pb_brasilmart.silver.items i ON o.order_id = i.order_id
# MAGIC   WHERE o.order_status IN ('delivered', 'shipped', 'invoiced', 'processing')
# MAGIC   GROUP BY DATE(o.order_purchase_timestamp)
# MAGIC   ORDER BY data_venda DESC
# MAGIC   LIMIT 5
# MAGIC )
# MAGIC SELECT
# MAGIC   data_venda,
# MAGIC   ROUND(gmv_recalculado, 2) AS gmv_da_silver,
# MAGIC   ROUND(ticket_recalculado, 2) AS ticket_da_silver
# MAGIC FROM silver_day;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Linhagem Visual — Unity Catalog Lineage Tab
# MAGIC
# MAGIC O Unity Catalog rastreia automaticamente a linhagem de dados quando:
# MAGIC - Tabelas são criadas via DLT (`dlt.read()` registra dependências)
# MAGIC - Queries SQL referenciam tabelas do catálogo
# MAGIC
# MAGIC ### Como visualizar:
# MAGIC 1. **Catalog Explorer** → `pb_brasilmart` → `silver` → `orders_enriched`
# MAGIC 2. Aba **Lineage** → mostra grafo de dependências upstream/downstream
# MAGIC 3. Clicar em uma coluna → mostra linhagem em nível de coluna
# MAGIC
# MAGIC ### Linhagem esperada no Catalog Explorer:
# MAGIC ```
# MAGIC UPSTREAM (fontes):
# MAGIC   bronze.orders ──→ silver.orders ──→ silver.orders_enriched
# MAGIC   bronze.customers ──→ silver.customers ──┘
# MAGIC   bronze.payments ──→ silver.payments ─────┘
# MAGIC
# MAGIC DOWNSTREAM (consumidores):
# MAGIC   silver.orders_enriched ──→ (Redshift raw_databricks.orders)
# MAGIC                            ──→ (dbt stg_orders → dim_clientes_rfm)
# MAGIC                            ──→ (dbt stg_orders → fato_vendas_diarias)
# MAGIC ```

# COMMAND ----------

# Demonstrar a linhagem programaticamente via metadata
print("=" * 70)
print("LINHAGEM DE DADOS — BrasilMart Data Platform")
print("=" * 70)

lineage_tempo = {
    "coluna": "tempo_total_seg",
    "descricao": "Velocidade de entrega (segundos do ciclo compra→entrega)",
    "camadas": [
        {
            "camada": "BRONZE",
            "tabela": "pb_brasilmart.bronze.orders",
            "colunas_origem": ["order_purchase_timestamp (STRING)", "order_delivered_customer_date (STRING)"],
            "transformacao": "Nenhuma (raw)",
        },
        {
            "camada": "SILVER",
            "tabela": "pb_brasilmart.silver.orders",
            "colunas_origem": ["tempo_total_seg (BIGINT)"],
            "transformacao": "unix_timestamp(delivered) - unix_timestamp(purchase)",
        },
        {
            "camada": "SILVER ENRICHED",
            "tabela": "pb_brasilmart.silver.orders_enriched",
            "colunas_origem": ["tempo_total_seg (BIGINT)"],
            "transformacao": "Passthrough via JOIN com silver.orders",
        },
        {
            "camada": "REDSHIFT STAGING",
            "tabela": "dev.pb_silver.stg_orders",
            "colunas_origem": ["tempo_total_seg (BIGINT)"],
            "transformacao": "Passthrough (VIEW dbt)",
        },
    ]
}

lineage_valor = {
    "coluna": "total_item_value → gmv",
    "descricao": "Valor total do item (price + frete) agregado em receita diaria",
    "camadas": [
        {
            "camada": "BRONZE",
            "tabela": "pb_brasilmart.bronze.items",
            "colunas_origem": ["price (STRING)", "freight_value (STRING)"],
            "transformacao": "Nenhuma (raw)",
        },
        {
            "camada": "SILVER",
            "tabela": "pb_brasilmart.silver.items",
            "colunas_origem": ["total_item_value (DECIMAL)"],
            "transformacao": "ROUND(CAST(price) + CAST(freight_value), 2)",
        },
        {
            "camada": "REDSHIFT STAGING",
            "tabela": "dev.pb_silver.stg_items",
            "colunas_origem": ["total_item_value"],
            "transformacao": "Passthrough (VIEW dbt)",
        },
        {
            "camada": "GOLD",
            "tabela": "dev.pb_gold.fato_vendas_diarias",
            "colunas_origem": ["gmv = SUM(total_item_value)", "ticket_medio = AVG(total_item_value)"],
            "transformacao": "Agregacao diaria (dbt incremental)",
        },
        {
            "camada": "GOLD",
            "tabela": "dev.pb_gold.dim_clientes_rfm",
            "colunas_origem": ["monetary = SUM(order_total)", "onde order_total = SUM(total_item_value)"],
            "transformacao": "Agregacao por cliente (dbt table)",
        },
    ]
}

for lineage in [lineage_tempo, lineage_valor]:
    print(f"\n{'─' * 70}")
    print(f"COLUNA: {lineage['coluna']}")
    print(f"  {lineage['descricao']}")
    print(f"{'─' * 70}")
    for i, step in enumerate(lineage["camadas"]):
        prefix = "  └─" if i == len(lineage["camadas"]) - 1 else "  ├─"
        arrow = " → " if i > 0 else "   "
        print(f"{prefix} [{step['camada']}] {step['tabela']}")
        print(f"  │   Colunas: {', '.join(step['colunas_origem'])}")
        print(f"  │   Transformacao: {step['transformacao']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Verificar Linhagem no Unity Catalog (Column-Level)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar as colunas da tabela orders_enriched e suas origens
# MAGIC DESCRIBE TABLE EXTENDED pb_brasilmart.silver.orders_enriched;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar que tempo_total_seg esta presente em cada camada
# MAGIC SELECT 'bronze.orders' AS camada,
# MAGIC        COUNT(*) AS total_rows,
# MAGIC        COUNT(order_purchase_timestamp) AS has_purchase,
# MAGIC        COUNT(order_delivered_customer_date) AS has_delivered,
# MAGIC        NULL AS tempo_total_seg_avg
# MAGIC FROM pb_brasilmart.bronze.orders

# MAGIC UNION ALL

# MAGIC SELECT 'silver.orders', COUNT(*), NULL, NULL,
# MAGIC        ROUND(AVG(tempo_total_seg), 0)
# MAGIC FROM pb_brasilmart.silver.orders

# MAGIC UNION ALL

# MAGIC SELECT 'silver.orders_enriched', COUNT(*), NULL, NULL,
# MAGIC        ROUND(AVG(tempo_total_seg), 0)
# MAGIC FROM pb_brasilmart.silver.orders_enriched;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumo TP4 — Atividade 2.3
# MAGIC
# MAGIC ### Row-Level Security
# MAGIC | Item | Detalhe |
# MAGIC |------|---------|
# MAGIC | Grupo | `Regiao_Norte` |
# MAGIC | Tabelas protegidas | `orders_enriched`, `customers`, `sellers` |
# MAGIC | Mecanismo | `IS_ACCOUNT_GROUP_MEMBER()` + `SET ROW FILTER` |
# MAGIC | Filtro | `customer_state IN ('AM','PA','AC','RO','RR','AP','TO')` |
# MAGIC | Admin | Vê todas as linhas |
# MAGIC | Regiao_Norte | Vê apenas linhas da região Norte |
# MAGIC
# MAGIC ### Linhagem de Dados
# MAGIC | Coluna | Caminho | Tipo |
# MAGIC |--------|---------|------|
# MAGIC | `tempo_total_seg` | Bronze(raw timestamps) → Silver(cálculo) → Enriched(join) → Redshift(staging) | Métrica de velocidade |
# MAGIC | `total_item_value` → `gmv` | Bronze(price+freight) → Silver(soma) → Staging(view) → Gold(agregação) | Receita end-to-end |
# MAGIC
# MAGIC ### Como ver a linhagem no Catalog Explorer
# MAGIC 1. Abrir **Catalog** → `pb_brasilmart` → `silver` → `orders`
# MAGIC 2. Clicar na aba **Lineage**
# MAGIC 3. Ver o grafo: `bronze.orders` → `silver.orders` → `silver.orders_enriched`
# MAGIC 4. Clicar na coluna `tempo_total_seg` para ver linhagem column-level
