#!/bin/bash
# TP4 2.1 — Descoberta e Marcacao de PII no AWS Glue Data Catalog
# Simula AWS Macie via tagging manual de colunas PII no Glue Data Catalog
# Uso: bash infra/aws/setup_pii_tagging.sh

set -euo pipefail

REGION="sa-east-1"
ACCOUNT_ID="234828142988"
CATALOG_ID="234828142988"

# Databases do Glue Data Catalog
DB_RAW="pb_raw_brasilmart"
DB_BRONZE="pb_bronze_brasilmart"

echo "================================================================"
echo "TP4 2.1 — Descoberta PII via Tagging no AWS Glue Data Catalog"
echo "================================================================"
echo ""

# ---------------------------------------------------------------
# Mapeamento PII do dataset Olist
# ---------------------------------------------------------------
# Classificacao LGPD:
#   - IDENTIFICADOR_DIRETO: dado que identifica a pessoa diretamente
#   - IDENTIFICADOR_INDIRETO: dado que, combinado, pode identificar a pessoa
#   - QUASI_IDENTIFICADOR: dado de localizacao aproximada
#   - TEXTO_LIVRE: campo de texto que pode conter PII nao estruturado
#   - DADO_FINANCEIRO: informacao de pagamento
# ---------------------------------------------------------------

echo "=== [1/5] Taggeando tabela: customers ==="
echo "  PII encontrado: customer_id (IDENTIFICADOR_DIRETO), customer_unique_id (IDENTIFICADOR_DIRETO),"
echo "                   customer_zip_code_prefix (QUASI_IDENTIFICADOR), customer_city (QUASI_IDENTIFICADOR)"

aws glue update-table \
  --catalog-id "$CATALOG_ID" \
  --database-name "$DB_BRONZE" \
  --table-input '{
    "Name": "customers",
    "Parameters": {
      "pii_detected": "true",
      "pii_classification": "LGPD_dados_pessoais",
      "pii_scan_date": "2026-06-23",
      "pii_scan_method": "manual_glue_tagging"
    },
    "StorageDescriptor": {
      "Columns": [
        {
          "Name": "customer_id",
          "Type": "string",
          "Comment": "PII:IDENTIFICADOR_DIRETO — Hash do CPF do cliente. Dado pessoal LGPD Art.5",
          "Parameters": {
            "pii_type": "IDENTIFICADOR_DIRETO",
            "pii_category": "CPF_HASH",
            "lgpd_base_legal": "consentimento",
            "masking_required": "true"
          }
        },
        {
          "Name": "customer_unique_id",
          "Type": "string",
          "Comment": "PII:IDENTIFICADOR_DIRETO — Identificador unico do cliente (hash). Dado pessoal LGPD",
          "Parameters": {
            "pii_type": "IDENTIFICADOR_DIRETO",
            "pii_category": "ID_PESSOAL",
            "lgpd_base_legal": "consentimento",
            "masking_required": "true"
          }
        },
        {
          "Name": "customer_zip_code_prefix",
          "Type": "string",
          "Comment": "PII:QUASI_IDENTIFICADOR — CEP parcial, pode identificar regiao do cliente",
          "Parameters": {
            "pii_type": "QUASI_IDENTIFICADOR",
            "pii_category": "CEP",
            "lgpd_base_legal": "interesse_legitimo",
            "masking_required": "false"
          }
        },
        {
          "Name": "customer_city",
          "Type": "string",
          "Comment": "PII:QUASI_IDENTIFICADOR — Cidade do cliente",
          "Parameters": {
            "pii_type": "QUASI_IDENTIFICADOR",
            "pii_category": "LOCALIZACAO",
            "masking_required": "false"
          }
        },
        {
          "Name": "customer_state",
          "Type": "string",
          "Comment": "NAO_PII — Estado (granularidade baixa)",
          "Parameters": {
            "pii_type": "NAO_PII"
          }
        }
      ],
      "Location": "s3://pb-bronze-brasilmart-234828142988/customers/",
      "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "SerdeInfo": {
        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      }
    }
  }' \
  --region "$REGION" 2>/dev/null && echo "  OK" || echo "  NOTA: Execute manualmente no Console se a tabela nao existir no Glue"

echo ""
echo "=== [2/5] Taggeando tabela: sellers ==="
echo "  PII encontrado: seller_id (IDENTIFICADOR_DIRETO), seller_zip_code_prefix (QUASI_IDENTIFICADOR)"

aws glue update-table \
  --catalog-id "$CATALOG_ID" \
  --database-name "$DB_BRONZE" \
  --table-input '{
    "Name": "sellers",
    "Parameters": {
      "pii_detected": "true",
      "pii_classification": "LGPD_dados_pessoais",
      "pii_scan_date": "2026-06-23",
      "pii_scan_method": "manual_glue_tagging"
    },
    "StorageDescriptor": {
      "Columns": [
        {
          "Name": "seller_id",
          "Type": "string",
          "Comment": "PII:IDENTIFICADOR_DIRETO — Hash do CNPJ/CPF do vendedor",
          "Parameters": {
            "pii_type": "IDENTIFICADOR_DIRETO",
            "pii_category": "CNPJ_CPF_HASH",
            "masking_required": "true"
          }
        },
        {
          "Name": "seller_zip_code_prefix",
          "Type": "string",
          "Comment": "PII:QUASI_IDENTIFICADOR — CEP parcial do vendedor",
          "Parameters": {
            "pii_type": "QUASI_IDENTIFICADOR",
            "pii_category": "CEP",
            "masking_required": "false"
          }
        },
        {
          "Name": "seller_city",
          "Type": "string",
          "Comment": "PII:QUASI_IDENTIFICADOR — Cidade do vendedor",
          "Parameters": {
            "pii_type": "QUASI_IDENTIFICADOR",
            "pii_category": "LOCALIZACAO",
            "masking_required": "false"
          }
        },
        {
          "Name": "seller_state",
          "Type": "string",
          "Comment": "NAO_PII",
          "Parameters": {
            "pii_type": "NAO_PII"
          }
        }
      ],
      "Location": "s3://pb-bronze-brasilmart-234828142988/sellers/",
      "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "SerdeInfo": {
        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      }
    }
  }' \
  --region "$REGION" 2>/dev/null && echo "  OK" || echo "  NOTA: Execute manualmente no Console se a tabela nao existir no Glue"

echo ""
echo "=== [3/5] Taggeando tabela: geolocation ==="
echo "  PII encontrado: geolocation_lat/lng (QUASI_IDENTIFICADOR — coordenadas GPS)"

aws glue update-table \
  --catalog-id "$CATALOG_ID" \
  --database-name "$DB_BRONZE" \
  --table-input '{
    "Name": "geolocation",
    "Parameters": {
      "pii_detected": "true",
      "pii_classification": "LGPD_dados_pessoais",
      "pii_scan_date": "2026-06-23",
      "pii_scan_method": "manual_glue_tagging"
    },
    "StorageDescriptor": {
      "Columns": [
        {
          "Name": "geolocation_zip_code_prefix",
          "Type": "string",
          "Comment": "PII:QUASI_IDENTIFICADOR — CEP",
          "Parameters": {
            "pii_type": "QUASI_IDENTIFICADOR",
            "pii_category": "CEP"
          }
        },
        {
          "Name": "geolocation_lat",
          "Type": "double",
          "Comment": "PII:QUASI_IDENTIFICADOR — Latitude GPS, pode identificar endereco exato",
          "Parameters": {
            "pii_type": "QUASI_IDENTIFICADOR",
            "pii_category": "COORDENADA_GPS",
            "masking_required": "true"
          }
        },
        {
          "Name": "geolocation_lng",
          "Type": "double",
          "Comment": "PII:QUASI_IDENTIFICADOR — Longitude GPS, pode identificar endereco exato",
          "Parameters": {
            "pii_type": "QUASI_IDENTIFICADOR",
            "pii_category": "COORDENADA_GPS",
            "masking_required": "true"
          }
        },
        {
          "Name": "geolocation_city",
          "Type": "string",
          "Comment": "PII:QUASI_IDENTIFICADOR — Cidade",
          "Parameters": {
            "pii_type": "QUASI_IDENTIFICADOR",
            "pii_category": "LOCALIZACAO"
          }
        },
        {
          "Name": "geolocation_state",
          "Type": "string",
          "Comment": "NAO_PII",
          "Parameters": {
            "pii_type": "NAO_PII"
          }
        }
      ],
      "Location": "s3://pb-bronze-brasilmart-234828142988/geolocation/",
      "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "SerdeInfo": {
        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      }
    }
  }' \
  --region "$REGION" 2>/dev/null && echo "  OK" || echo "  NOTA: Execute manualmente no Console se a tabela nao existir no Glue"

echo ""
echo "=== [4/5] Taggeando tabela: reviews ==="
echo "  PII encontrado: review_comment_message (TEXTO_LIVRE — pode conter nomes, emails, telefones)"

aws glue update-table \
  --catalog-id "$CATALOG_ID" \
  --database-name "$DB_BRONZE" \
  --table-input '{
    "Name": "reviews",
    "Parameters": {
      "pii_detected": "true",
      "pii_classification": "LGPD_potencial_dado_pessoal",
      "pii_scan_date": "2026-06-23",
      "pii_scan_method": "manual_glue_tagging"
    },
    "StorageDescriptor": {
      "Columns": [
        {
          "Name": "review_id",
          "Type": "string",
          "Comment": "NAO_PII — Identificador da review",
          "Parameters": { "pii_type": "NAO_PII" }
        },
        {
          "Name": "order_id",
          "Type": "string",
          "Comment": "PII:IDENTIFICADOR_INDIRETO — Linkavel ao customer_id",
          "Parameters": {
            "pii_type": "IDENTIFICADOR_INDIRETO",
            "pii_category": "CHAVE_ESTRANGEIRA"
          }
        },
        {
          "Name": "review_score",
          "Type": "int",
          "Comment": "NAO_PII",
          "Parameters": { "pii_type": "NAO_PII" }
        },
        {
          "Name": "review_comment_title",
          "Type": "string",
          "Comment": "PII:TEXTO_LIVRE — Titulo pode conter dados pessoais",
          "Parameters": {
            "pii_type": "TEXTO_LIVRE",
            "pii_category": "CONTEUDO_USUARIO",
            "masking_required": "true"
          }
        },
        {
          "Name": "review_comment_message",
          "Type": "string",
          "Comment": "PII:TEXTO_LIVRE — Mensagem pode conter nome, email, telefone, endereco",
          "Parameters": {
            "pii_type": "TEXTO_LIVRE",
            "pii_category": "CONTEUDO_USUARIO",
            "masking_required": "true"
          }
        },
        {
          "Name": "review_creation_date",
          "Type": "string",
          "Comment": "NAO_PII",
          "Parameters": { "pii_type": "NAO_PII" }
        },
        {
          "Name": "review_answer_timestamp",
          "Type": "string",
          "Comment": "NAO_PII",
          "Parameters": { "pii_type": "NAO_PII" }
        }
      ],
      "Location": "s3://pb-bronze-brasilmart-234828142988/reviews/",
      "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "SerdeInfo": {
        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      }
    }
  }' \
  --region "$REGION" 2>/dev/null && echo "  OK" || echo "  NOTA: Execute manualmente no Console se a tabela nao existir no Glue"

echo ""
echo "=== [5/5] Taggeando tabela: orders (chave estrangeira PII) ==="
echo "  PII encontrado: customer_id (IDENTIFICADOR_INDIRETO — FK para customers)"

aws glue update-table \
  --catalog-id "$CATALOG_ID" \
  --database-name "$DB_BRONZE" \
  --table-input '{
    "Name": "orders",
    "Parameters": {
      "pii_detected": "true",
      "pii_classification": "LGPD_dado_indireto",
      "pii_scan_date": "2026-06-23",
      "pii_scan_method": "manual_glue_tagging"
    },
    "StorageDescriptor": {
      "Columns": [
        {
          "Name": "order_id",
          "Type": "string",
          "Comment": "NAO_PII — Identificador do pedido",
          "Parameters": { "pii_type": "NAO_PII" }
        },
        {
          "Name": "customer_id",
          "Type": "string",
          "Comment": "PII:IDENTIFICADOR_INDIRETO — FK para customers (hash CPF)",
          "Parameters": {
            "pii_type": "IDENTIFICADOR_INDIRETO",
            "pii_category": "CHAVE_ESTRANGEIRA_PII",
            "masking_required": "false"
          }
        },
        {
          "Name": "order_status",
          "Type": "string",
          "Comment": "NAO_PII",
          "Parameters": { "pii_type": "NAO_PII" }
        },
        {
          "Name": "order_purchase_timestamp",
          "Type": "string",
          "Comment": "NAO_PII",
          "Parameters": { "pii_type": "NAO_PII" }
        },
        {
          "Name": "order_approved_at",
          "Type": "string",
          "Comment": "NAO_PII",
          "Parameters": { "pii_type": "NAO_PII" }
        },
        {
          "Name": "order_delivered_carrier_date",
          "Type": "string",
          "Comment": "NAO_PII",
          "Parameters": { "pii_type": "NAO_PII" }
        },
        {
          "Name": "order_delivered_customer_date",
          "Type": "string",
          "Comment": "NAO_PII",
          "Parameters": { "pii_type": "NAO_PII" }
        },
        {
          "Name": "order_estimated_delivery_date",
          "Type": "string",
          "Comment": "NAO_PII",
          "Parameters": { "pii_type": "NAO_PII" }
        }
      ],
      "Location": "s3://pb-bronze-brasilmart-234828142988/orders/",
      "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "SerdeInfo": {
        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      }
    }
  }' \
  --region "$REGION" 2>/dev/null && echo "  OK" || echo "  NOTA: Execute manualmente no Console se a tabela nao existir no Glue"

echo ""
echo "================================================================"
echo "Resumo da Descoberta PII — BrasilMart (Dataset Olist)"
echo "================================================================"
echo ""
echo "Tabelas com PII detectado:"
echo ""
echo "  TABELA         | COLUNA                    | TIPO PII              | CATEGORIA"
echo "  -------------- | ------------------------- | --------------------- | ---------"
echo "  customers      | customer_id               | IDENTIFICADOR_DIRETO  | CPF_HASH"
echo "  customers      | customer_unique_id        | IDENTIFICADOR_DIRETO  | ID_PESSOAL"
echo "  customers      | customer_zip_code_prefix   | QUASI_IDENTIFICADOR   | CEP"
echo "  customers      | customer_city             | QUASI_IDENTIFICADOR   | LOCALIZACAO"
echo "  sellers        | seller_id                 | IDENTIFICADOR_DIRETO  | CNPJ_CPF_HASH"
echo "  sellers        | seller_zip_code_prefix     | QUASI_IDENTIFICADOR   | CEP"
echo "  sellers        | seller_city               | QUASI_IDENTIFICADOR   | LOCALIZACAO"
echo "  geolocation    | geolocation_lat           | QUASI_IDENTIFICADOR   | COORDENADA_GPS"
echo "  geolocation    | geolocation_lng           | QUASI_IDENTIFICADOR   | COORDENADA_GPS"
echo "  geolocation    | geolocation_zip_code_prefix| QUASI_IDENTIFICADOR   | CEP"
echo "  reviews        | review_comment_title      | TEXTO_LIVRE           | CONTEUDO_USUARIO"
echo "  reviews        | review_comment_message    | TEXTO_LIVRE           | CONTEUDO_USUARIO"
echo "  orders         | customer_id               | IDENTIFICADOR_INDIRETO| CHAVE_ESTRANGEIRA"
echo ""
echo "Total: 5 tabelas taggeadas, 13 colunas PII identificadas"
echo "Metodo: Tagging manual no Glue Data Catalog (simulacao AWS Macie)"
echo "Classificacao: LGPD (Lei Geral de Protecao de Dados)"
