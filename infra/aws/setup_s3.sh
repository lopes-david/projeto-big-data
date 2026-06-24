#!/bin/bash
# ============================================================
# Setup S3 Buckets — BrasilMart Data Lakehouse
# Alternativa ao Terraform para criação rápida via AWS CLI
# ============================================================

set -euo pipefail

PROJECT="pb-brasilmart"
ENV="${1:-dev}"
REGION="us-east-1"

BUCKETS=("raw" "bronze" "silver" "gold")

echo "Criando buckets para o ambiente: ${ENV}"

for LAYER in "${BUCKETS[@]}"; do
    BUCKET_NAME="${PROJECT}-${LAYER}-${ENV}"
    echo "→ Criando bucket: ${BUCKET_NAME}"

    # Criar bucket
    aws s3api create-bucket \
        --bucket "${BUCKET_NAME}" \
        --region "${REGION}" \
        2>/dev/null || echo "  Bucket já existe"

    # Habilitar versionamento
    aws s3api put-bucket-versioning \
        --bucket "${BUCKET_NAME}" \
        --versioning-configuration Status=Enabled

    # Bloquear acesso público
    aws s3api put-public-access-block \
        --bucket "${BUCKET_NAME}" \
        --public-access-block-configuration \
            BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

    # Habilitar criptografia SSE-S3
    aws s3api put-bucket-encryption \
        --bucket "${BUCKET_NAME}" \
        --server-side-encryption-configuration '{
            "Rules": [{
                "ApplyServerSideEncryptionByDefault": {
                    "SSEAlgorithm": "AES256"
                },
                "BucketKeyEnabled": true
            }]
        }'

    echo "  ✓ Bucket ${BUCKET_NAME} configurado"
done

# Aplicar políticas de ciclo de vida
echo ""
echo "Aplicando políticas de ciclo de vida..."

# Raw: Standard → IA (90d) → Glacier (365d) → Expiração (2555d = 7 anos)
aws s3api put-bucket-lifecycle-configuration \
    --bucket "${PROJECT}-raw-${ENV}" \
    --lifecycle-configuration file://lifecycle_raw.json

# Bronze: Standard → IA (90d) → Glacier (730d) → Expiração (1095d = 3 anos)
aws s3api put-bucket-lifecycle-configuration \
    --bucket "${PROJECT}-bronze-${ENV}" \
    --lifecycle-configuration file://lifecycle_bronze.json

# Silver: Standard → IA (180d) → Expiração (730d = 2 anos)
aws s3api put-bucket-lifecycle-configuration \
    --bucket "${PROJECT}-silver-${ENV}" \
    --lifecycle-configuration file://lifecycle_silver.json

# Gold: Expiração (365d = 1 ano, dados re-processados periodicamente)
aws s3api put-bucket-lifecycle-configuration \
    --bucket "${PROJECT}-gold-${ENV}" \
    --lifecycle-configuration file://lifecycle_gold.json

echo ""
echo "Criando estrutura de diretórios..."

for LAYER in "${BUCKETS[@]}"; do
    BUCKET_NAME="${PROJECT}-${LAYER}-${ENV}"
    for DIR in orders customers products sellers payments reviews geolocation; do
        aws s3api put-object --bucket "${BUCKET_NAME}" --key "${DIR}/" > /dev/null
    done
    echo "  ✓ Diretórios criados em ${BUCKET_NAME}"
done

echo ""
echo "Setup S3 concluído!"
