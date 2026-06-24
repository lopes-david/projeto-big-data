# Databricks notebook source
# MAGIC %md
# MAGIC # TP3 — 1.1 / 1.2 / 1.4 Pipeline DLT — Camada Silver (Limpeza + Enriquecimento + Qualidade)
# MAGIC
# MAGIC **Objetivo:** Pipeline **Delta Live Tables (DLT)** que lê as 9 tabelas da Camada Bronze
# MAGIC (`pb_brasilmart.bronze`), aplica limpeza, calcula campos derivados e enriquece
# MAGIC os dados com joins de tabelas de cadastro.
# MAGIC
# MAGIC **Configuração do pipeline (Databricks UI):**
# MAGIC - Target catalog: `pb_brasilmart`
# MAGIC - Target schema: `silver`
# MAGIC - Source: este notebook
# MAGIC
# MAGIC ### Parte 1 — Limpeza (Bronze → Silver base)
# MAGIC | Tabela | Limpeza |
# MAGIC |--------|---------|
# MAGIC | orders | Trim/lower status, **tempos decorridos em segundos** entre eventos, classificação prazo |
# MAGIC | customers | CEP formatado (5 dígitos), cidade lower, estado upper |
# MAGIC | items | Valores monetários arredondados, total calculado |
# MAGIC | payments | Tipo agrupado (cartão/boleto/voucher), valor > 0 |
# MAGIC | reviews | Sentimento, flag comentário, **tempo resposta em segundos** |
# MAGIC | products | Categoria traduzida EN, peso em kg, dimensões, **volume cm³ calculado** |
# MAGIC | sellers | CEP, cidade e estado padronizados |
# MAGIC | geolocation | Deduplicação por CEP, coordenadas médias |
# MAGIC | category_translation | Nomes trimados |
# MAGIC
# MAGIC ### Parte 2 — Enriquecimento (Joins com cadastros)
# MAGIC | Tabela Enriquecida | Fontes Silver | Campos adicionais |
# MAGIC |--------------------|---------------|-------------------|
# MAGIC | orders_enriched | orders + customers + payments | cidade/estado do cliente, total pago, método principal |
# MAGIC | items_enriched | items + products + sellers | categoria, peso, cidade/estado do vendedor |
# MAGIC
# MAGIC ### Parte 3 — Qualidade de Dados (TP3 1.4) — Três ações de Expectations
# MAGIC | Regra | Ação | Tabela | Comportamento |
# MAGIC |-------|------|--------|---------------|
# MAGIC | `Valor_Pagamento_Valido` | **DROP ROW** | payments | Descarta pagamentos fora do range R$0,01–R$99.999 |
# MAGIC | `Order_ID_Obrigatorio` | **FAIL UPDATE** | orders_enriched | Interrompe pipeline se order_id for nulo (integridade crítica) |
# MAGIC | `Entrega_Atraso_Alerta` | **WARN** | orders | Alerta entregas com >30 dias de atraso, mas mantém o registro |

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Silver Orders (Pedidos)
# MAGIC
# MAGIC Campos calculados de **tempo decorrido em segundos** entre cada etapa do ciclo de vida:
# MAGIC - `tempo_aprovacao_seg`: compra → aprovação
# MAGIC - `tempo_postagem_seg`: aprovação → postagem na transportadora
# MAGIC - `tempo_transporte_seg`: postagem → entrega ao cliente
# MAGIC - `tempo_total_seg`: compra → entrega ao cliente

# COMMAND ----------

@dlt.table(
    name="orders",
    comment="Pedidos limpos com tempos decorridos (segundos) entre eventos e status de entrega",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect_or_drop("customer_id_not_null", "customer_id IS NOT NULL")
@dlt.expect("Entrega_Atraso_Alerta",
    "delta_entrega_dias <= 30 OR delta_entrega_dias IS NULL"
)
def silver_orders():
    df = spark.table("pb_brasilmart.bronze.orders")

    purchase = F.col("order_purchase_timestamp").cast("timestamp")
    approved = F.col("order_approved_at").cast("timestamp")
    carrier  = F.col("order_delivered_carrier_date").cast("timestamp")
    delivered = F.col("order_delivered_customer_date").cast("timestamp")
    estimated = F.col("order_estimated_delivery_date").cast("timestamp")

    return (
        df.select(
            F.col("order_id"),
            F.col("customer_id"),
            F.lower(F.trim(F.col("order_status"))).alias("order_status"),
            purchase.alias("order_purchase_timestamp"),
            approved.alias("order_approved_at"),
            carrier.alias("order_delivered_carrier_date"),
            delivered.alias("order_delivered_customer_date"),
            estimated.alias("order_estimated_delivery_date"),

            (F.unix_timestamp(approved) - F.unix_timestamp(purchase)).alias("tempo_aprovacao_seg"),
            (F.unix_timestamp(carrier) - F.unix_timestamp(approved)).alias("tempo_postagem_seg"),
            (F.unix_timestamp(delivered) - F.unix_timestamp(carrier)).alias("tempo_transporte_seg"),
            (F.unix_timestamp(delivered) - F.unix_timestamp(purchase)).alias("tempo_total_seg"),

            F.datediff(delivered, estimated).alias("delta_entrega_dias"),

            F.when(delivered <= estimated, F.lit("no_prazo"))
             .when(delivered > estimated, F.lit("atrasado"))
             .otherwise(F.lit("pendente")).alias("status_entrega"),

            F.current_timestamp().alias("_transformado_em")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver Customers (Clientes)

# COMMAND ----------

@dlt.table(
    name="customers",
    comment="Clientes com estado/cidade padronizados e CEP formatado",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("customer_id_not_null", "customer_id IS NOT NULL")
def silver_customers():
    return (
        spark.table("pb_brasilmart.bronze.customers")
        .select(
            F.col("customer_id"),
            F.col("customer_unique_id"),
            F.lpad(F.col("customer_zip_code_prefix").cast("string"), 5, "0").alias("customer_zip_code"),
            F.lower(F.trim(F.col("customer_city"))).alias("customer_city"),
            F.upper(F.trim(F.col("customer_state"))).alias("customer_state"),
            F.current_timestamp().alias("_transformado_em")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver Items (Itens de Pedido)

# COMMAND ----------

@dlt.table(
    name="items",
    comment="Itens de pedido com valores monetários validados e total calculado",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect_or_drop("product_id_not_null", "product_id IS NOT NULL")
@dlt.expect("Preco_Alerta_Alto",
    "price < 10000"
)
def silver_items():
    return (
        spark.table("pb_brasilmart.bronze.items")
        .select(
            F.col("order_id"),
            F.col("order_item_id"),
            F.col("product_id"),
            F.col("seller_id"),
            F.col("shipping_limit_date").cast("timestamp").alias("shipping_limit_date"),
            F.round(F.col("price").cast("decimal(10,2)"), 2).alias("price"),
            F.round(F.col("freight_value").cast("decimal(10,2)"), 2).alias("freight_value"),
            F.round(
                (F.col("price").cast("decimal(10,2)") + F.col("freight_value").cast("decimal(10,2)")), 2
            ).alias("total_item_value"),
            F.current_timestamp().alias("_transformado_em")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver Payments (Pagamentos)

# COMMAND ----------

@dlt.table(
    name="payments",
    comment="Pagamentos com tipo agrupado e valores validados",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect_or_drop("Valor_Pagamento_Valido",
    "payment_value BETWEEN 0.01 AND 99999.99"
)
def silver_payments():
    return (
        spark.table("pb_brasilmart.bronze.payments")
        .select(
            F.col("order_id"),
            F.col("payment_sequential"),
            F.lower(F.trim(F.col("payment_type"))).alias("payment_type"),
            F.col("payment_installments"),
            F.round(F.col("payment_value").cast("decimal(10,2)"), 2).alias("payment_value"),
            F.when(F.lower(F.trim(F.col("payment_type"))).isin("credit_card", "debit_card"), F.lit("cartao"))
             .when(F.lower(F.trim(F.col("payment_type"))) == "boleto", F.lit("boleto"))
             .when(F.lower(F.trim(F.col("payment_type"))) == "voucher", F.lit("voucher"))
             .otherwise(F.lit("outro")).alias("payment_group"),
            F.current_timestamp().alias("_transformado_em")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver Reviews (Avaliações)
# MAGIC
# MAGIC Campo calculado: `tempo_resposta_seg` — segundos entre criação e resposta da review.

# COMMAND ----------

@dlt.table(
    name="reviews",
    comment="Avaliações com sentimento, flag de comentário e tempo de resposta em segundos",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("review_id_not_null", "review_id IS NOT NULL")
@dlt.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect("Score_Range_Valido",
    "review_score BETWEEN 1 AND 5"
)
def silver_reviews():
    creation = F.col("review_creation_date").cast("timestamp")
    answer   = F.col("review_answer_timestamp").cast("timestamp")
    score    = F.col("review_score").cast("int")

    return (
        spark.table("pb_brasilmart.bronze.reviews")
        .select(
            F.col("review_id"),
            F.col("order_id"),
            score.alias("review_score"),
            F.coalesce(F.col("review_comment_title"), F.lit("")).alias("review_title"),
            F.coalesce(F.col("review_comment_message"), F.lit("")).alias("review_message"),
            creation.alias("review_creation_date"),
            answer.alias("review_answer_timestamp"),

            (F.unix_timestamp(answer) - F.unix_timestamp(creation)).alias("tempo_resposta_seg"),

            F.when(score >= 4, F.lit("positivo"))
             .when(score == 3, F.lit("neutro"))
             .otherwise(F.lit("negativo")).alias("review_sentiment"),
            F.when(
                (F.col("review_comment_message").isNotNull()) &
                (F.length(F.col("review_comment_message")) > 0),
                F.lit(True)
            ).otherwise(F.lit(False)).alias("has_comment"),
            F.current_timestamp().alias("_transformado_em")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver Products (Produtos)

# COMMAND ----------

@dlt.table(
    name="products",
    comment="Produtos com categoria EN, peso kg, volume cm³ e classificação de porte",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("product_id_not_null", "product_id IS NOT NULL")
def silver_products():
    df_products = spark.table("pb_brasilmart.bronze.products")
    df_translation = spark.table("pb_brasilmart.bronze.category_translation")

    length_cm = F.col("p.product_length_cm").cast("decimal(10,2)")
    height_cm = F.col("p.product_height_cm").cast("decimal(10,2)")
    width_cm  = F.col("p.product_width_cm").cast("decimal(10,2)")
    weight_g  = F.col("p.product_weight_g").cast("decimal(10,2)")
    volume    = F.round(length_cm * height_cm * width_cm, 1)

    return (
        df_products.alias("p")
        .join(
            df_translation.alias("t"),
            F.col("p.product_category_name") == F.col("t.product_category_name"),
            "left"
        )
        .select(
            F.col("p.product_id"),
            F.coalesce(
                F.col("t.product_category_name_english"),
                F.regexp_replace(F.col("p.product_category_name"), "_", " ")
            ).alias("product_category"),
            F.col("p.product_name_lenght").alias("product_name_length"),
            F.col("p.product_description_lenght").alias("product_description_length"),
            F.col("p.product_photos_qty").cast("int").alias("product_photos_qty"),
            F.round(weight_g / 1000, 2).alias("product_weight_kg"),
            F.round(length_cm, 1).alias("product_length_cm"),
            F.round(height_cm, 1).alias("product_height_cm"),
            F.round(width_cm, 1).alias("product_width_cm"),
            volume.alias("product_volume_cm3"),
            F.when(weight_g <= 500, F.lit("pequeno"))
             .when(weight_g <= 5000, F.lit("medio"))
             .otherwise(F.lit("grande")).alias("porte_produto"),
            F.current_timestamp().alias("_transformado_em")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Silver Sellers (Vendedores)

# COMMAND ----------

@dlt.table(
    name="sellers",
    comment="Vendedores com cidade/estado padronizados e CEP formatado",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("seller_id_not_null", "seller_id IS NOT NULL")
def silver_sellers():
    return (
        spark.table("pb_brasilmart.bronze.sellers")
        .select(
            F.col("seller_id"),
            F.lpad(F.col("seller_zip_code_prefix").cast("string"), 5, "0").alias("seller_zip_code"),
            F.lower(F.trim(F.col("seller_city"))).alias("seller_city"),
            F.upper(F.trim(F.col("seller_state"))).alias("seller_state"),
            F.current_timestamp().alias("_transformado_em")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Silver Geolocation (Geolocalização)

# COMMAND ----------

@dlt.table(
    name="geolocation",
    comment="Geolocalização deduplicada por CEP com coordenadas médias",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("zip_code_not_null", "geolocation_zip_code_prefix IS NOT NULL")
def silver_geolocation():
    return (
        spark.table("pb_brasilmart.bronze.geolocation")
        .groupBy(
            F.lpad(F.col("geolocation_zip_code_prefix").cast("string"), 5, "0").alias("geolocation_zip_code_prefix"),
            F.lower(F.trim(F.col("geolocation_city"))).alias("geolocation_city"),
            F.upper(F.trim(F.col("geolocation_state"))).alias("geolocation_state")
        )
        .agg(
            F.round(F.avg("geolocation_lat"), 6).alias("geolocation_lat"),
            F.round(F.avg("geolocation_lng"), 6).alias("geolocation_lng"),
            F.count("*").alias("num_registros")
        )
        .withColumn("_transformado_em", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Silver Category Translation (Tradução de Categorias)

# COMMAND ----------

@dlt.table(
    name="category_translation",
    comment="Tradução de categorias de produto português → inglês",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("category_pt_not_null", "product_category_name IS NOT NULL")
@dlt.expect_or_drop("category_en_not_null", "product_category_name_english IS NOT NULL")
def silver_category_translation():
    return (
        spark.table("pb_brasilmart.bronze.category_translation")
        .select(
            F.trim(F.col("product_category_name")).alias("product_category_name"),
            F.trim(F.col("product_category_name_english")).alias("product_category_name_english"),
            F.current_timestamp().alias("_transformado_em")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Parte 2 — Enriquecimento (Joins com tabelas de cadastro)
# MAGIC
# MAGIC Tabelas que combinam dados de múltiplas fontes Silver usando `dlt.read()`,
# MAGIC criando visões enriquecidas prontas para consumo analítico.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Orders Enriched (Pedidos + Clientes + Pagamentos)
# MAGIC
# MAGIC Enriquece cada pedido com:
# MAGIC - **Cadastro do cliente**: cidade, estado, CEP (tabela `customers`)
# MAGIC - **Resumo de pagamento**: valor total pago, nº de parcelas, método principal (tabela `payments`)

# COMMAND ----------

@dlt.table(
    name="orders_enriched",
    comment="Pedidos enriquecidos com dados do cliente e resumo de pagamento",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_fail("Order_ID_Obrigatorio",
    "order_id IS NOT NULL"
)
@dlt.expect_or_fail("Customer_ID_Obrigatorio",
    "customer_id IS NOT NULL"
)
def silver_orders_enriched():
    df_orders = dlt.read("orders")
    df_customers = dlt.read("customers")

    df_payments_agg = (
        dlt.read("payments")
        .groupBy("order_id")
        .agg(
            F.round(F.sum("payment_value"), 2).alias("total_pago"),
            F.max("payment_installments").alias("max_parcelas"),
            F.count("*").alias("num_pagamentos"),
            F.first("payment_type").alias("metodo_pagamento_principal"),
            F.first("payment_group").alias("grupo_pagamento_principal")
        )
    )

    return (
        df_orders.alias("o")
        .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "left")
        .join(df_payments_agg.alias("p"), F.col("o.order_id") == F.col("p.order_id"), "left")
        .select(
            F.col("o.order_id"),
            F.col("o.customer_id"),
            F.col("c.customer_unique_id"),
            F.col("o.order_status"),
            F.col("o.order_purchase_timestamp"),
            F.col("o.order_approved_at"),
            F.col("o.order_delivered_carrier_date"),
            F.col("o.order_delivered_customer_date"),
            F.col("o.order_estimated_delivery_date"),

            F.col("o.tempo_aprovacao_seg"),
            F.col("o.tempo_postagem_seg"),
            F.col("o.tempo_transporte_seg"),
            F.col("o.tempo_total_seg"),
            F.col("o.delta_entrega_dias"),
            F.col("o.status_entrega"),

            F.col("c.customer_city"),
            F.col("c.customer_state"),
            F.col("c.customer_zip_code"),

            F.col("p.total_pago"),
            F.col("p.max_parcelas"),
            F.col("p.num_pagamentos"),
            F.col("p.metodo_pagamento_principal"),
            F.col("p.grupo_pagamento_principal"),

            F.current_timestamp().alias("_transformado_em")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Items Enriched (Itens + Produtos + Vendedores)
# MAGIC
# MAGIC Enriquece cada item de pedido com:
# MAGIC - **Cadastro do produto**: categoria, peso, volume, porte (tabela `products`)
# MAGIC - **Cadastro do vendedor**: cidade, estado (tabela `sellers`)

# COMMAND ----------

@dlt.table(
    name="items_enriched",
    comment="Itens de pedido enriquecidos com dados do produto e do vendedor",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_fail("Item_Order_ID_Obrigatorio",
    "order_id IS NOT NULL"
)
@dlt.expect_or_fail("Item_Product_ID_Obrigatorio",
    "product_id IS NOT NULL"
)
def silver_items_enriched():
    df_items = dlt.read("items")
    df_products = dlt.read("products")
    df_sellers = dlt.read("sellers")

    return (
        df_items.alias("i")
        .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.product_id"), "left")
        .join(df_sellers.alias("s"), F.col("i.seller_id") == F.col("s.seller_id"), "left")
        .select(
            F.col("i.order_id"),
            F.col("i.order_item_id"),
            F.col("i.product_id"),
            F.col("i.seller_id"),
            F.col("i.shipping_limit_date"),
            F.col("i.price"),
            F.col("i.freight_value"),
            F.col("i.total_item_value"),

            F.col("p.product_category"),
            F.col("p.product_weight_kg"),
            F.col("p.product_volume_cm3"),
            F.col("p.porte_produto"),

            F.col("s.seller_city"),
            F.col("s.seller_state"),
            F.col("s.seller_zip_code"),

            F.current_timestamp().alias("_transformado_em")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumo do Pipeline DLT Silver
# MAGIC
# MAGIC ### Parte 1 — Tabelas base (limpeza + campos calculados)
# MAGIC | Tabela Silver | Fonte Bronze | Transformações | Expectativas |
# MAGIC |---------------|-------------|----------------|-------------|
# MAGIC | `orders` | `bronze.orders` | Trim, lower, **4 tempos em seg**, delta entrega, status | DROP: order_id, customer_id ・ **WARN: Entrega_Atraso_Alerta** |
# MAGIC | `customers` | `bronze.customers` | CEP 5 dígitos, cidade lower, estado upper | DROP: customer_id |
# MAGIC | `items` | `bronze.items` | Decimal, total calculado | DROP: order_id, product_id ・ **WARN: Preco_Alerta_Alto** |
# MAGIC | `payments` | `bronze.payments` | Tipo agrupado, arredondamento | DROP: order_id ・ **DROP: Valor_Pagamento_Valido** |
# MAGIC | `reviews` | `bronze.reviews` | Sentimento, flag comentário, **tempo resposta seg** | DROP: review_id, order_id ・ **WARN: Score_Range_Valido** |
# MAGIC | `products` | `bronze.products` + `category_translation` | Categoria EN, peso kg, **volume cm³, porte** | DROP: product_id |
# MAGIC | `sellers` | `bronze.sellers` | CEP, cidade, estado padronizados | DROP: seller_id |
# MAGIC | `geolocation` | `bronze.geolocation` | Dedup por CEP, coordenadas médias | DROP: zip_code |
# MAGIC | `category_translation` | `bronze.category_translation` | Trim nomes | DROP: ambas colunas |
# MAGIC
# MAGIC ### Parte 2 — Tabelas enriquecidas (joins com cadastros)
# MAGIC | Tabela Enriquecida | Fontes Silver (via `dlt.read`) | Expectativas |
# MAGIC |--------------------|-------------------------------|-------------|
# MAGIC | `orders_enriched` | orders + customers + payments | **FAIL: Order_ID_Obrigatorio, Customer_ID_Obrigatorio** |
# MAGIC | `items_enriched` | items + products + sellers | **FAIL: Item_Order_ID_Obrigatorio, Item_Product_ID_Obrigatorio** |
# MAGIC
# MAGIC ### Parte 3 — Qualidade de Dados (TP3 1.4) — Três ações de Expectations
# MAGIC
# MAGIC | # | Regra | Decorator | Ação (ON VIOLATION) | Tabela | Lógica |
# MAGIC |---|-------|-----------|---------------------|--------|--------|
# MAGIC | 1.4.1 | `Valor_Pagamento_Valido` | `@dlt.expect_or_drop` | **DROP ROW** | payments | `payment_value BETWEEN 0.01 AND 99999.99` — descarta pagamentos com valor fora da faixa válida |
# MAGIC | 1.4.2 | `Order_ID_Obrigatorio` | `@dlt.expect_or_fail` | **FAIL UPDATE** | orders_enriched | `order_id IS NOT NULL` — interrompe o pipeline se a chave primária for nula (integridade crítica) |
# MAGIC | 1.4.3 | `Entrega_Atraso_Alerta` | `@dlt.expect` | **WARN** | orders | `delta_entrega_dias <= 30 OR delta_entrega_dias IS NULL` — alerta entregas com atraso extremo (>30 dias) mas mantém o registro |
# MAGIC
# MAGIC **Expectations adicionais de WARN:**
# MAGIC | Regra | Tabela | Lógica |
# MAGIC |-------|--------|--------|
# MAGIC | `Score_Range_Valido` | reviews | `review_score BETWEEN 1 AND 5` — alerta scores fora do range esperado |
# MAGIC | `Preco_Alerta_Alto` | items | `price < 10000` — alerta itens com preço acima de R$10.000 |
# MAGIC
# MAGIC ### Campos calculados de tempo decorrido
# MAGIC | Campo | Descrição |
# MAGIC |-------|-----------|
# MAGIC | `tempo_aprovacao_seg` | Segundos entre compra e aprovação |
# MAGIC | `tempo_postagem_seg` | Segundos entre aprovação e postagem na transportadora |
# MAGIC | `tempo_transporte_seg` | Segundos entre postagem e entrega ao cliente |
# MAGIC | `tempo_total_seg` | Segundos entre compra e entrega (ciclo completo) |
# MAGIC | `tempo_resposta_seg` | Segundos entre criação e resposta da review |
# MAGIC
# MAGIC ### Grafo de dependências DLT
# MAGIC ```
# MAGIC bronze.orders ──────→ silver.orders ─────┐
# MAGIC bronze.customers ───→ silver.customers ──┤
# MAGIC bronze.payments ────→ silver.payments ───┴→ silver.orders_enriched
# MAGIC
# MAGIC bronze.items ───────→ silver.items ──────┐
# MAGIC bronze.products ─┐                       │
# MAGIC bronze.category ─┴→ silver.products ─────┤
# MAGIC bronze.sellers ────→ silver.sellers ──────┴→ silver.items_enriched
# MAGIC ```
# MAGIC
# MAGIC ### Como executar no Databricks
# MAGIC
# MAGIC 1. **Workflows → Delta Live Tables → Create Pipeline**
# MAGIC 2. Pipeline name: `pb-brasilmart-silver`
# MAGIC 3. Source code: este notebook
# MAGIC 4. Target catalog: `pb_brasilmart`
# MAGIC 5. Target schema: `silver`
# MAGIC 6. Cluster mode: `Enhanced` (recomendado para Unity Catalog)
# MAGIC 7. **Start** → o pipeline cria as **11 tabelas** Silver automaticamente (9 base + 2 enriquecidas)
