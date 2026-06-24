#!/bin/bash
# ============================================================================
# TP5 — Atividade 4.1: Dashboard QuickSight conectado ao Redshift
#
# Este script configura o Amazon QuickSight para o projeto BrasilMart:
#   1. Cria a Data Source (conexão com Redshift Serverless)
#   2. Cria os DataSets (queries SQL sobre as tabelas Gold)
#   3. Cria a Analysis e o Dashboard
#
# Pré-requisitos:
#   - QuickSight Enterprise habilitado na conta AWS
#   - QuickSight com acesso ao Redshift (VPC/Security Group configurado)
#   - Variáveis de ambiente: REDSHIFT_PASSWORD
# ============================================================================

set -euo pipefail

AWS_ACCOUNT_ID="234828142988"
AWS_REGION="sa-east-1"
QS_REGION="us-east-1"  # QuickSight pode estar em região diferente
QS_USER="admin/david"
QS_NAMESPACE="default"

REDSHIFT_HOST="default-workgroup.${AWS_ACCOUNT_ID}.${AWS_REGION}.redshift-serverless.amazonaws.com"
REDSHIFT_PORT=5439
REDSHIFT_DB="dev"
REDSHIFT_USER="admin"
REDSHIFT_PASSWORD="${REDSHIFT_PASSWORD:?Defina REDSHIFT_PASSWORD}"

DS_ID="pb-brasilmart-redshift-ds"
DASHBOARD_ID="pb-brasilmart-dashboard-tp5"

echo "================================================="
echo "  QuickSight Setup — BrasilMart Dashboard (TP5)"
echo "================================================="

# -------------------------------------------------------------------
# 1. Criar Data Source (conexão Redshift)
# -------------------------------------------------------------------
echo ""
echo "1. Criando Data Source: ${DS_ID}"

aws quicksight create-data-source \
  --region "${QS_REGION}" \
  --aws-account-id "${AWS_ACCOUNT_ID}" \
  --data-source-id "${DS_ID}" \
  --name "BrasilMart Redshift Gold" \
  --type REDSHIFT \
  --data-source-parameters '{
    "RedshiftParameters": {
      "Host": "'"${REDSHIFT_HOST}"'",
      "Port": '"${REDSHIFT_PORT}"',
      "Database": "'"${REDSHIFT_DB}"'"
    }
  }' \
  --credentials '{
    "CredentialPair": {
      "Username": "'"${REDSHIFT_USER}"'",
      "Password": "'"${REDSHIFT_PASSWORD}"'"
    }
  }' \
  --permissions '[{
    "Principal": "arn:aws:quicksight:'"${QS_REGION}"':'"${AWS_ACCOUNT_ID}"':user/'"${QS_NAMESPACE}"'/'"${QS_USER}"'",
    "Actions": [
      "quicksight:DescribeDataSource",
      "quicksight:DescribeDataSourcePermissions",
      "quicksight:PassDataSource",
      "quicksight:UpdateDataSource",
      "quicksight:UpdateDataSourcePermissions",
      "quicksight:DeleteDataSource"
    ]
  }]' \
  2>/dev/null && echo "  Data Source criado." || echo "  Data Source já existe (ou erro)."

# -------------------------------------------------------------------
# 2. Criar DataSets (um para cada aba do dashboard)
# -------------------------------------------------------------------
echo ""
echo "2. Criando DataSets..."

create_dataset() {
  local DATASET_ID=$1
  local DATASET_NAME=$2
  local SQL_QUERY=$3

  aws quicksight create-data-set \
    --region "${QS_REGION}" \
    --aws-account-id "${AWS_ACCOUNT_ID}" \
    --data-set-id "${DATASET_ID}" \
    --name "${DATASET_NAME}" \
    --import-mode DIRECT_QUERY \
    --physical-table-map '{
      "'"${DATASET_ID}"'": {
        "CustomSql": {
          "DataSourceArn": "arn:aws:quicksight:'"${QS_REGION}"':'"${AWS_ACCOUNT_ID}"':datasource/'"${DS_ID}"'",
          "Name": "'"${DATASET_NAME}"'",
          "SqlQuery": "'"${SQL_QUERY}"'",
          "Columns": []
        }
      }
    }' \
    --permissions '[{
      "Principal": "arn:aws:quicksight:'"${QS_REGION}"':'"${AWS_ACCOUNT_ID}"':user/'"${QS_NAMESPACE}"'/'"${QS_USER}"'",
      "Actions": [
        "quicksight:DescribeDataSet",
        "quicksight:DescribeDataSetPermissions",
        "quicksight:PassDataSet",
        "quicksight:DescribeIngestion",
        "quicksight:ListIngestions",
        "quicksight:UpdateDataSet",
        "quicksight:DeleteDataSet",
        "quicksight:CreateIngestion",
        "quicksight:CancelIngestion",
        "quicksight:UpdateDataSetPermissions"
      ]
    }]' \
    2>/dev/null && echo "  Dataset '${DATASET_NAME}' criado." || echo "  Dataset '${DATASET_NAME}' já existe."
}

# --- 2.1 KPI: Vendas Diárias (R4 — TP1 Q5) ---
create_dataset "ds-vendas-diarias" "Vendas Diárias (GMV)" \
  "SELECT data_venda, total_pedidos, total_clientes, gmv, ticket_medio, total_frete FROM pb_gold.fato_vendas_diarias ORDER BY data_venda"

# --- 2.2 KPI: Segmentação RFM (R1 — TP1 Q1, Q2) ---
create_dataset "ds-clientes-rfm" "Segmentação RFM Clientes" \
  "SELECT customer_unique_id, customer_state, customer_city, frequency, monetary, recency_days, avg_ticket, first_purchase, last_purchase, rfm_segment FROM pb_gold.dim_clientes_rfm"

# --- 2.3 KPI: Score de Vendedores (R3 — TP1 Q4) ---
create_dataset "ds-sellers-score" "Score de Vendedores" \
  "SELECT seller_id, seller_city, seller_state, total_orders, delivered_orders, canceled_orders, avg_review_score, on_time_rate, cancel_rate, seller_score, seller_tier FROM pb_gold.dim_sellers_score"

# --- 2.4 KPI: Performance de Produtos (R7 — TP1 Q8) ---
create_dataset "ds-produtos-perf" "Performance de Produtos" \
  "SELECT product_id, product_category, total_orders, total_revenue, avg_price, days_since_last_sale, avg_review_score, total_reviews, negative_review_rate, product_status FROM pb_gold.dim_produtos_performance"

# --- 2.5 Feedback Loop: Predições ML (R2 — TP1 Q3) ---
create_dataset "ds-predicoes-ml" "Predições ML Atraso" \
  "SELECT order_id, customer_id, customer_state, seller_state, total_pago, total_frete, qtd_itens, peso_medio_kg, label, predicao_atraso, probabilidade_falha, risco_atraso, modelo_versao, scored_at FROM pb_gold.predicoes_databricks_ml"

echo ""
echo "  5 DataSets criados."

# -------------------------------------------------------------------
# 3. Queries SQL para cada visualização do Dashboard
# -------------------------------------------------------------------
echo ""
echo "3. Queries SQL documentadas abaixo (para criação manual das visuals)."
echo ""
echo "As queries estão documentadas no arquivo docs/tp5_dashboard_queries.sql"
echo "e no notebook tp5_04_dashboard_quicksight.py"

echo ""
echo "================================================="
echo "  Setup concluído!"
echo "  Próximo passo: abrir QuickSight Console e criar"
echo "  as visualizações usando os DataSets acima."
echo "================================================="
