#!/bin/bash
# ============================================================
# Setup AWS Lake Formation + Glue Data Catalog
# BrasilMart Data Platform — Governança Inicial
# ============================================================

set -euo pipefail

PROJECT="pb-brasilmart"
ENV="${1:-dev}"
REGION="us-east-1"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

echo "Configurando Lake Formation e Glue Data Catalog..."

# -------------------------------------------------------
# 1. Criar databases no Glue Data Catalog (um por camada)
# -------------------------------------------------------
echo ""
echo "1. Criando databases no Glue Data Catalog..."

declare -A DB_DESCRIPTIONS=(
    ["raw"]="Dados brutos originais Olist (orders, customers, products...) - BrasilMart"
    ["bronze"]="Dados convertidos para Parquet/Delta sem transformação"
    ["silver"]="Dados limpos, padronizados e deduplicados"
    ["gold"]="Tabelas analíticas, KPIs e visão 360° do cliente"
)

for LAYER in raw bronze silver gold; do
    DB_NAME="${PROJECT}_${LAYER}"
    echo "  → Criando database: ${DB_NAME}"

    aws glue create-database \
        --region "${REGION}" \
        --database-input "{
            \"Name\": \"${DB_NAME}\",
            \"Description\": \"${DB_DESCRIPTIONS[$LAYER]}\",
            \"LocationUri\": \"s3://${PROJECT}-${LAYER}-${ENV}/\",
            \"Parameters\": {
                \"project\": \"brasilmart\",
                \"environment\": \"${ENV}\",
                \"layer\": \"${LAYER}\"
            }
        }" 2>/dev/null || echo "    Database já existe"

    echo "    ✓ ${DB_NAME}"
done

# -------------------------------------------------------
# 2. Registrar buckets S3 como locations no Lake Formation
# -------------------------------------------------------
echo ""
echo "2. Registrando locations no Lake Formation..."

for LAYER in raw bronze silver gold; do
    BUCKET_ARN="arn:aws:s3:::${PROJECT}-${LAYER}-${ENV}"
    echo "  → Registrando: ${BUCKET_ARN}"

    aws lakeformation register-resource \
        --resource-arn "${BUCKET_ARN}" \
        --use-service-linked-role \
        --region "${REGION}" \
        2>/dev/null || echo "    Location já registrada"
done

# -------------------------------------------------------
# 3. Configurar permissões do Lake Formation
# -------------------------------------------------------
echo ""
echo "3. Configurando permissões Lake Formation..."

# Permissão para o role de ETL (Glue) acessar todas as camadas
GLUE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${PROJECT}-glue-etl-role"

for LAYER in raw bronze silver gold; do
    DB_NAME="${PROJECT}_${LAYER}"
    aws lakeformation grant-permissions \
        --principal "DataLakePrincipalIdentifier=${GLUE_ROLE_ARN}" \
        --resource "{\"Database\": {\"Name\": \"${DB_NAME}\"}}" \
        --permissions "ALL" \
        --region "${REGION}" \
        2>/dev/null || echo "    Permissão já existe para ${DB_NAME}"
done

# Permissão de analista: apenas SELECT em Silver e Gold
ANALYST_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${PROJECT}-data-analyst"

for LAYER in silver gold; do
    DB_NAME="${PROJECT}_${LAYER}"
    aws lakeformation grant-permissions \
        --principal "DataLakePrincipalIdentifier=${ANALYST_ROLE_ARN}" \
        --resource "{\"Database\": {\"Name\": \"${DB_NAME}\"}}" \
        --permissions "DESCRIBE" \
        --region "${REGION}" \
        2>/dev/null || echo "    Permissão já existe para ${DB_NAME}"

    aws lakeformation grant-permissions \
        --principal "DataLakePrincipalIdentifier=${ANALYST_ROLE_ARN}" \
        --resource "{\"Table\": {\"DatabaseName\": \"${DB_NAME}\", \"TableWildcard\": {}}}" \
        --permissions "SELECT" "DESCRIBE" \
        --region "${REGION}" \
        2>/dev/null || echo "    Permissão de tabela já existe"
done

# -------------------------------------------------------
# 4. Criar Crawler para a camada Raw
# -------------------------------------------------------
echo ""
echo "4. Criando Crawler para camada Raw..."

aws glue create-crawler \
    --name "${PROJECT}-raw-crawler" \
    --role "${GLUE_ROLE_ARN}" \
    --database-name "${PROJECT}_raw" \
    --targets "{
        \"S3Targets\": [
            {\"Path\": \"s3://${PROJECT}-raw-${ENV}/orders/\"},
            {\"Path\": \"s3://${PROJECT}-raw-${ENV}/customers/\"},
            {\"Path\": \"s3://${PROJECT}-raw-${ENV}/products/\"},
            {\"Path\": \"s3://${PROJECT}-raw-${ENV}/orders_json/\"}
        ]
    }" \
    --schema-change-policy "{
        \"UpdateBehavior\": \"UPDATE_IN_DATABASE\",
        \"DeleteBehavior\": \"LOG\"
    }" \
    --region "${REGION}" \
    2>/dev/null || echo "  Crawler já existe"

echo "  ✓ Crawler criado"

echo ""
echo "Setup Lake Formation e Glue Data Catalog concluído!"
echo ""
echo "Próximos passos:"
echo "  1. Execute o crawler: aws glue start-crawler --name ${PROJECT}-raw-crawler"
echo "  2. Verifique as tabelas: aws glue get-tables --database-name ${PROJECT}_raw"
