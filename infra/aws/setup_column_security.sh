#!/bin/bash
# TP4 2.2 — Permissoes Finas: Lake Formation Column-Level Security para Analista Jr.
# Restringe acesso a colunas PII (customer_id, seller_id, GPS, reviews) no Athena
# Uso: bash infra/aws/setup_column_security.sh

set -euo pipefail

REGION="sa-east-1"
ACCOUNT_ID="234828142988"
DB_BRONZE="pb_bronze_brasilmart"
DB_SILVER="pb_silver_brasilmart"
DB_GOLD="pb_gold_brasilmart"

echo "================================================================"
echo "TP4 2.2 — Column-Level Security via Lake Formation"
echo "================================================================"

# ---------------------------------------------------------------
# 1. Criar IAM Role para Analista Jr.
# ---------------------------------------------------------------
echo ""
echo "=== [1/5] Criando IAM Role: pb-brasilmart-analista-jr ==="

TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::'"$ACCOUNT_ID"':root"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "aws:PrincipalTag/cargo": "analista_junior"
        }
      }
    },
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lakeformation.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}'

aws iam create-role \
  --role-name "pb-brasilmart-analista-jr" \
  --assume-role-policy-document "$TRUST_POLICY" \
  --tags Key=Project,Value=pb-brasilmart Key=TP,Value=tp4 Key=cargo,Value=analista_junior \
  --region "$REGION" 2>/dev/null && echo "  Role criada" || echo "  Role ja existe"

ANALYST_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/pb-brasilmart-analista-jr"

# Política inline: Athena + Glue + S3 (somente leitura)
echo "  Anexando politica de acesso Athena/Glue/S3..."

aws iam put-role-policy \
  --role-name "pb-brasilmart-analista-jr" \
  --policy-name "PBAnalistaJrAthenaAccess" \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "AthenaAccess",
        "Effect": "Allow",
        "Action": [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:ListWorkGroups"
        ],
        "Resource": "*"
      },
      {
        "Sid": "GlueReadOnly",
        "Effect": "Allow",
        "Action": [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartitions"
        ],
        "Resource": "*"
      },
      {
        "Sid": "S3ReadData",
        "Effect": "Allow",
        "Action": [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ],
        "Resource": [
          "arn:aws:s3:::pb-silver-brasilmart-234828142988",
          "arn:aws:s3:::pb-silver-brasilmart-234828142988/*",
          "arn:aws:s3:::pb-gold-brasilmart-234828142988",
          "arn:aws:s3:::pb-gold-brasilmart-234828142988/*"
        ]
      },
      {
        "Sid": "AthenaResults",
        "Effect": "Allow",
        "Action": [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ],
        "Resource": [
          "arn:aws:s3:::aws-athena-query-results-234828142988-sa-east-1",
          "arn:aws:s3:::aws-athena-query-results-234828142988-sa-east-1/*"
        ]
      },
      {
        "Sid": "LakeFormationAccess",
        "Effect": "Allow",
        "Action": [
          "lakeformation:GetDataAccess"
        ],
        "Resource": "*"
      }
    ]
  }' 2>/dev/null && echo "  Politica anexada" || echo "  Politica ja existe"

# ---------------------------------------------------------------
# 2. Revogar permissões IAM default (forcar Lake Formation)
# ---------------------------------------------------------------
echo ""
echo "=== [2/5] Configurando Lake Formation como controle principal ==="
echo "  NOTA: Desabilite IAMAllowedPrincipals no Console Lake Formation"
echo "  Settings → Data catalog settings → Desmarcar 'Use only IAM'"

# ---------------------------------------------------------------
# 3. Conceder acesso DESCRIBE nos databases Silver e Gold
# ---------------------------------------------------------------
echo ""
echo "=== [3/5] Concedendo DESCRIBE nos databases Silver e Gold ==="

for DB in "$DB_SILVER" "$DB_GOLD"; do
  aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${ANALYST_ROLE_ARN}" \
    --resource "{\"Database\": {\"Name\": \"${DB}\"}}" \
    --permissions "DESCRIBE" \
    --region "$REGION" 2>/dev/null && echo "  OK: DESCRIBE em $DB" || echo "  Permissao ja existe: $DB"
done

# ---------------------------------------------------------------
# 4. Column-Level Permissions — EXCLUIR colunas PII
# ---------------------------------------------------------------
echo ""
echo "=== [4/5] Aplicando Column-Level Security (excluindo PII) ==="
echo ""

# --- customers: excluir customer_id e customer_unique_id ---
echo "  [customers] Excluindo: customer_id, customer_unique_id"
aws lakeformation grant-permissions \
  --principal "DataLakePrincipalIdentifier=${ANALYST_ROLE_ARN}" \
  --resource '{
    "TableWithColumns": {
      "DatabaseName": "'"$DB_BRONZE"'",
      "Name": "customers",
      "ColumnWildcard": {
        "ExcludedColumnNames": ["customer_id", "customer_unique_id"]
      }
    }
  }' \
  --permissions "SELECT" \
  --region "$REGION" 2>/dev/null && echo "    OK" || echo "    Permissao ja existe"

echo "  [customers Silver] Excluindo: customer_id, customer_unique_id"
aws lakeformation grant-permissions \
  --principal "DataLakePrincipalIdentifier=${ANALYST_ROLE_ARN}" \
  --resource '{
    "TableWithColumns": {
      "DatabaseName": "'"$DB_SILVER"'",
      "Name": "customers",
      "ColumnWildcard": {
        "ExcludedColumnNames": ["customer_id", "customer_unique_id"]
      }
    }
  }' \
  --permissions "SELECT" \
  --region "$REGION" 2>/dev/null && echo "    OK" || echo "    Permissao ja existe"

# --- sellers: excluir seller_id ---
echo "  [sellers] Excluindo: seller_id"
aws lakeformation grant-permissions \
  --principal "DataLakePrincipalIdentifier=${ANALYST_ROLE_ARN}" \
  --resource '{
    "TableWithColumns": {
      "DatabaseName": "'"$DB_BRONZE"'",
      "Name": "sellers",
      "ColumnWildcard": {
        "ExcludedColumnNames": ["seller_id"]
      }
    }
  }' \
  --permissions "SELECT" \
  --region "$REGION" 2>/dev/null && echo "    OK" || echo "    Permissao ja existe"

echo "  [sellers Silver] Excluindo: seller_id"
aws lakeformation grant-permissions \
  --principal "DataLakePrincipalIdentifier=${ANALYST_ROLE_ARN}" \
  --resource '{
    "TableWithColumns": {
      "DatabaseName": "'"$DB_SILVER"'",
      "Name": "sellers",
      "ColumnWildcard": {
        "ExcludedColumnNames": ["seller_id"]
      }
    }
  }' \
  --permissions "SELECT" \
  --region "$REGION" 2>/dev/null && echo "    OK" || echo "    Permissao ja existe"

# --- geolocation: excluir lat/lng (coordenadas GPS) ---
echo "  [geolocation] Excluindo: geolocation_lat, geolocation_lng"
aws lakeformation grant-permissions \
  --principal "DataLakePrincipalIdentifier=${ANALYST_ROLE_ARN}" \
  --resource '{
    "TableWithColumns": {
      "DatabaseName": "'"$DB_BRONZE"'",
      "Name": "geolocation",
      "ColumnWildcard": {
        "ExcludedColumnNames": ["geolocation_lat", "geolocation_lng"]
      }
    }
  }' \
  --permissions "SELECT" \
  --region "$REGION" 2>/dev/null && echo "    OK" || echo "    Permissao ja existe"

# --- reviews: excluir texto livre (pode conter PII) ---
echo "  [reviews] Excluindo: review_comment_title, review_comment_message"
aws lakeformation grant-permissions \
  --principal "DataLakePrincipalIdentifier=${ANALYST_ROLE_ARN}" \
  --resource '{
    "TableWithColumns": {
      "DatabaseName": "'"$DB_BRONZE"'",
      "Name": "reviews",
      "ColumnWildcard": {
        "ExcludedColumnNames": ["review_comment_title", "review_comment_message"]
      }
    }
  }' \
  --permissions "SELECT" \
  --region "$REGION" 2>/dev/null && echo "    OK" || echo "    Permissao ja existe"

# --- orders, items, payments, products: acesso completo (sem PII direto) ---
echo ""
echo "  [orders, items, payments, products] Acesso completo (sem PII direto)"
for TABLE in orders items payments products; do
  aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${ANALYST_ROLE_ARN}" \
    --resource '{
      "TableWithColumns": {
        "DatabaseName": "'"$DB_BRONZE"'",
        "Name": "'"$TABLE"'",
        "ColumnWildcard": {}
      }
    }' \
    --permissions "SELECT" \
    --region "$REGION" 2>/dev/null && echo "    OK: $TABLE" || echo "    Permissao ja existe: $TABLE"
done

# --- Gold: acesso completo (dados agregados, sem PII) ---
echo ""
echo "  [Gold] Acesso completo a todas as tabelas (dados agregados)"
aws lakeformation grant-permissions \
  --principal "DataLakePrincipalIdentifier=${ANALYST_ROLE_ARN}" \
  --resource "{\"Table\": {\"DatabaseName\": \"${DB_GOLD}\", \"TableWildcard\": {}}}" \
  --permissions "SELECT" "DESCRIBE" \
  --region "$REGION" 2>/dev/null && echo "    OK: todas tabelas Gold" || echo "    Permissao ja existe"

# ---------------------------------------------------------------
# 5. Resumo
# ---------------------------------------------------------------
echo ""
echo "================================================================"
echo "Resumo — Column-Level Security para Analista Jr."
echo "================================================================"
echo ""
echo "Role: $ANALYST_ROLE_ARN"
echo ""
echo "PERMISSOES POR TABELA:"
echo "  ┌──────────────┬──────────────────────────────────┬──────────────────────────────┐"
echo "  │ Tabela       │ Colunas VISIVEIS                 │ Colunas BLOQUEADAS (PII)     │"
echo "  ├──────────────┼──────────────────────────────────┼──────────────────────────────┤"
echo "  │ customers    │ zip_code_prefix, city, state      │ customer_id, unique_id       │"
echo "  │ sellers      │ zip_code_prefix, city, state      │ seller_id                    │"
echo "  │ geolocation  │ zip_code_prefix, city, state      │ geolocation_lat, lng         │"
echo "  │ reviews      │ review_id, order_id, score, dates │ comment_title, message       │"
echo "  │ orders       │ TODAS                            │ (nenhuma)                    │"
echo "  │ items        │ TODAS                            │ (nenhuma)                    │"
echo "  │ payments     │ TODAS                            │ (nenhuma)                    │"
echo "  │ products     │ TODAS                            │ (nenhuma)                    │"
echo "  │ Gold (todas) │ TODAS                            │ (nenhuma — dados agregados)  │"
echo "  └──────────────┴──────────────────────────────────┴──────────────────────────────┘"
echo ""
echo "TESTE NO ATHENA (como Analista Jr.):"
echo "  -- Deve funcionar (colunas nao-PII):"
echo "  SELECT customer_zip_code_prefix, customer_city, customer_state"
echo "    FROM pb_bronze_brasilmart.customers LIMIT 10;"
echo ""
echo "  -- Deve FALHAR (coluna PII bloqueada):"
echo "  SELECT customer_id FROM pb_bronze_brasilmart.customers LIMIT 10;"
echo "  --> AccessDeniedException: Insufficient Lake Formation permission(s)"
