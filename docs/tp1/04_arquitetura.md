# TP1 - Parte 1.3: Arquitetura da Solução

## Visão Geral

A arquitetura integra serviços AWS (S3, Glue, Lake Formation, Redshift) com Databricks, formando um Data Lakehouse híbrido que suporta ingestão batch dos 9 datasets Olist, processamento distribuído com Spark, governança centralizada e consumo analítico para visão 360° do cliente.

---

## Diagrama da Arquitetura

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                   PLATAFORMA DE DADOS BRASILMART (Olist Dataset)                     │
│                        Arquitetura Data Lakehouse Híbrida                            │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ╔══════════════════════════════════════════════════════════╗                        │
│  ║                  FONTES DE DADOS (Olist)                 ║                        │
│  ╚══════════════════════════════════════════════════════════╝                        │
│                                                                                     │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐                  │
│  │ orders   │ │customers │ │ products │ │ sellers  │ │geolocation│                  │
│  │ 99.441   │ │ 99.441   │ │  32.951  │ │  3.095   │ │1.000.163  │                  │
│  │  (CSV)   │ │  (CSV)   │ │  (CSV)   │ │  (CSV)   │ │  (CSV)   │                  │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘                  │
│       │             │            │             │             │                        │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐                                             │
│  │ payments │ │ reviews  │ │ items    │   ← Gerado: JSON aninhado (pedido unificado) │
│  │ 103.886  │ │ 104.719  │ │ 112.650  │                                             │
│  │  (CSV)   │ │  (CSV)   │ │  (CSV)   │                                             │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘                                             │
│       │             │            │                                                   │
│       ▼             ▼            ▼                                                   │
│  ╔══════════════════════════════════════════════════════════════╗                    │
│  ║                    CAMADA DE INGESTÃO                        ║                    │
│  ╠══════════════════════════════════════════════════════════════╣                    │
│  ║                                                              ║                    │
│  ║  ┌────────────────────────┐    ┌──────────────────────────┐  ║                    │
│  ║  │    AWS Glue Studio     │    │   Databricks (PySpark)   │  ║                    │
│  ║  │    (Batch ETL Job)     │    │                          │  ║                    │
│  ║  │                        │    │ • JSON aninhado (pedido  │  ║                    │
│  ║  │ • olist_orders.csv     │    │   = order + items +      │  ║                    │
│  ║  │   → Parquet (bronze)   │    │   payments + review)     │  ║                    │
│  ║  │ • Schema inference     │    │ • Streaming simulado     │  ║                    │
│  ║  │ • Job Bookmark         │    │   (orders por timestamp) │  ║                    │
│  ║  │   (incremental)        │    │ • Auto Loader S3         │  ║                    │
│  ║  └──────────┬─────────────┘    └───────────┬──────────────┘  ║                    │
│  ║             │                              │                  ║                    │
│  ╚═════════════╪══════════════════════════════╪══════════════════╝                    │
│                │                              │                                       │
│                ▼                              ▼                                       │
│  ╔══════════════════════════════════════════════════════════════╗                    │
│  ║              AMAZON S3 — DATA LAKEHOUSE (4 CAMADAS)          ║                    │
│  ╠══════════════════════════════════════════════════════════════╣                    │
│  ║                                                              ║                    │
│  ║  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       ║                    │
│  ║  │ brasilmart-  │  │ brasilmart-  │  │ brasilmart-  │       ║                    │
│  ║  │    raw/      │  │   bronze/    │  │   silver/    │       ║                    │
│  ║  │              │  │              │  │              │       ║                    │
│  ║  │ CSVs originais│  │ Parquet/Delta│  │ Dados limpos │       ║                    │
│  ║  │ da Olist     │──▶│ sem limpeza  │──▶│ padronizados │       ║                    │
│  ║  │ (imutável)   │  │ + metadados  │  │ deduplicados │       ║                    │
│  ║  │ 7 anos       │  │ 3 anos       │  │ 2 anos       │       ║                    │
│  ║  └──────────────┘  └──────────────┘  └──────┬───────┘       ║                    │
│  ║                                              │               ║                    │
│  ║                                    ┌─────────▼────────┐     ║                    │
│  ║                                    │  brasilmart-gold/ │     ║                    │
│  ║                                    │                   │     ║                    │
│  ║                                    │ • dim_clientes_rfm│     ║                    │
│  ║                                    │ • dim_sellers_score│    ║                    │
│  ║                                    │ • fato_vendas_dia │     ║                    │
│  ║                                    │ • dim_produtos_perf│    ║                    │
│  ║                                    └──────────┬────────┘     ║                    │
│  ║                                               │              ║                    │
│  ╚═══════════════════════════════════════════════╪══════════════╝                    │
│                                                  │                                   │
│  ╔═══════════════════════════════════════════════╪══════════════╗                    │
│  ║         PROCESSAMENTO — DATABRICKS + SPARK    │              ║                    │
│  ╠═══════════════════════════════════════════════╪══════════════╣                    │
│  ║                                               │              ║                    │
│  ║  ┌────────────────────────────────────────┐   │              ║                    │
│  ║  │          DATABRICKS WORKSPACE           │   │              ║                    │
│  ║  │                                        │   │              ║                    │
│  ║  │  ┌────────────┐  ┌──────────────────┐  │   │              ║                    │
│  ║  │  │All-Purpose │  │  Jobs Cluster    │  │   │              ║                    │
│  ║  │  │  Cluster   │  │ (2-8 workers,    │  │   │              ║                    │
│  ║  │  │  (Dev)     │  │  spot instances) │  │   │              ║                    │
│  ║  │  └────────────┘  └──────────────────┘  │   │              ║                    │
│  ║  │                                        │   │              ║                    │
│  ║  │  Notebooks:                            │   │              ║                    │
│  ║  │  01 - Ingestão JSON aninhado (pedidos) │   │              ║                    │
│  ║  │  02 - Streaming simulado (orders/time) │   │              ║                    │
│  ║  │  03 - Limpeza e qualidade Bronze       │   │              ║                    │
│  ║  └────────────────────────────────────────┘   │              ║                    │
│  ╚═══════════════════════════════════════════════╪══════════════╝                    │
│                                                  │                                   │
│  ╔═══════════════════════════════════════════════╪══════════════╗                    │
│  ║                       CONSUMO                 │              ║                    │
│  ╠═══════════════════════════════════════════════╪══════════════╣                    │
│  ║  ┌─────────────┐  ┌─────────────┐  ┌──────────▼──────────┐  ║                    │
│  ║  │  Power BI   │  │ Databricks  │  │    Amazon Redshift   │  ║                    │
│  ║  │  Tableau    │  │     SQL     │  │     Serverless       │  ║                    │
│  ║  │             │  │  (ad-hoc)   │  │                      │  ║                    │
│  ║  │ Dashboards  │  │             │  │ Queries SQL rápidas  │  ║                    │
│  ║  │ RFM, GMV,   │  │ Análise     │  │ sobre camada Gold    │  ║                    │
│  ║  │ Sellers     │  │ exploratória│  │                      │  ║                    │
│  ║  └─────────────┘  └─────────────┘  └─────────────────────┘  ║                    │
│  ╚══════════════════════════════════════════════════════════════╝                    │
│                                                                                     │
│  ╔══════════════════════════════════════════════════════════════╗                    │
│  ║          GOVERNANÇA E SEGURANÇA (TRANSVERSAL / LGPD)         ║                    │
│  ╠══════════════════════════════════════════════════════════════╣                    │
│  ║  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  ║                    │
│  ║  │ Lake Form.  │  │ Glue Data   │  │    AWS IAM + KMS     │  ║                    │
│  ║  │             │  │  Catalog    │  │                      │  ║                    │
│  ║  │ Row/Column  │  │ Schemas,    │  │ Roles por perfil     │  ║                    │
│  ║  │ security    │  │ partições,  │  │ SSE-KMS em todos     │  ║                    │
│  ║  │ Audit logs  │  │ linhagem    │  │ os buckets           │  ║                    │
│  ║  └─────────────┘  └─────────────┘  └─────────────────────┘  ║                    │
│  ╚══════════════════════════════════════════════════════════════╝                    │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Mapeamento Olist → Arquitetura

### Datasets e Camadas

| Dataset Olist | Camada Raw | Processamento | Camada Gold |
|---------------|-----------|---------------|-------------|
| `olist_orders_dataset.csv` | `raw/orders/` | Glue Studio (batch) | `fato_vendas_diarias`, `fato_logistica` |
| `olist_customers_dataset.csv` | `raw/customers/` | Glue Studio (batch) | `dim_clientes_rfm` |
| `olist_order_items_dataset.csv` | `raw/order_items/` | Databricks (join) | `fato_receita_por_produto` |
| `olist_order_payments_dataset.csv` | `raw/payments/` | Databricks (join) | `fato_pagamentos` |
| `olist_order_reviews_dataset.csv` | `raw/reviews/` | Databricks (NLP) | `dim_reviews_score` |
| `olist_products_dataset.csv` | `raw/products/` | Glue Studio | `dim_produtos_performance` |
| `olist_sellers_dataset.csv` | `raw/sellers/` | Glue Studio | `dim_sellers_score` |
| `olist_geolocation_dataset.csv` | `raw/geolocation/` | Databricks (Spark) | `dim_geo_clientes` |
| **JSON Unificado** (gerado) | `raw/orders_json/` | Databricks (01) | Base para visão 360° |

### JSON Aninhado — Estrutura do Pedido Unificado

Para atender o requisito de "ingestão de dados complexos (JSON aninhado)", um script de preparação combina 4 CSVs em documentos JSON aninhados (estrutura típica de API de e-commerce):

```json
{
  "order_id": "e481f51cbdc54678b7cc49136f2d6af7",
  "order_status": "delivered",
  "customer": {
    "customer_id": "9ef432eb6251297304e76186b10a928d",
    "customer_unique_id": "861eff4711a542e4b93843c6dd7febb0",
    "zip_code_prefix": "14409",
    "city": "franca",
    "state": "SP"
  },
  "timestamps": {
    "purchase": "2017-10-02 10:56:33",
    "approved": "2017-10-02 11:07:15",
    "delivered_carrier": "2017-10-04 19:55:00",
    "delivered_customer": "2017-10-10 21:25:13",
    "estimated_delivery": "2017-10-18 00:00:00"
  },
  "items": [
    {
      "order_item_id": 1,
      "product_id": "4244733e06e7ecb4970a6e2683c13e61",
      "seller_id": "48436dade18ac8b2bce089ec2a041202",
      "shipping_limit_date": "2017-10-06 11:07:15",
      "price": 58.90,
      "freight_value": 13.29
    }
  ],
  "payments": [
    {
      "sequential": 1,
      "type": "credit_card",
      "installments": 3,
      "value": 72.19
    }
  ],
  "review": {
    "review_id": "7bc2406110b926393aa56f80a40eba40",
    "score": 4,
    "comment_title": null,
    "comment_message": null,
    "creation_date": "2017-10-18 00:00:00",
    "answer_timestamp": "2017-10-20 14:38:59"
  }
}
```

---

## Buckets S3

| Bucket | Camada | Conteúdo | Lifecycle |
|--------|--------|----------|-----------|
| `brasilmart-raw-dev` | Raw | CSVs originais da Olist + JSON gerado | Standard → Glacier (1 ano) → Exp. (7 anos) |
| `brasilmart-bronze-dev` | Bronze | Parquet + Delta sem transformação | Standard → IA (90d) → Glacier (2 anos) |
| `brasilmart-silver-dev` | Silver | Delta Lake limpo e padronizado | Standard → IA (180d) → Exp. (2 anos) |
| `brasilmart-gold-dev` | Gold | Delta tabelas analíticas finais | Standard → Exp. (1 ano, reprocessado) |

---

## Fluxo End-to-End

```
1. EXTRAÇÃO     2. PREPARAÇÃO      3. INGESTÃO        4. BRONZE       5. SILVER/GOLD
   CSVs Olist ──▶ Script Python  ──▶ Glue Studio    ──▶ Parquet/  ──▶ Tabelas
   (raw/)         (CSV → JSON        (orders/          Delta          analíticas
                  aninhado)          customers)         Bronze         (RFM, GMV,
                                  + Databricks                        Sellers)
                                   (JSON aninhado
                                    + Streaming)
                                         │
                           ┌─────────────┘
                           │  Glue Data Catalog (schemas + partições)
                           │  Lake Formation (acesso por perfil/LGPD)
                           └─────────────────────────────────────────
```
