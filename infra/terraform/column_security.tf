# TP4 2.2 — Column-Level Security via Lake Formation para Analista Jr.

# IAM Role para Analista Jr.
resource "aws_iam_role" "analista_jr" {
  name = "pb-brasilmart-analista-jr"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${var.account_id}:root"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "aws:PrincipalTag/cargo" = "analista_junior"
          }
        }
      },
      {
        Effect = "Allow"
        Principal = {
          Service = "lakeformation.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    TP    = "tp4"
    cargo = "analista_junior"
  }
}

resource "aws_iam_role_policy" "analista_jr_athena" {
  name = "PBAnalistaJrAthenaAccess"
  role = aws_iam_role.analista_jr.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AthenaAccess"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:ListWorkGroups"
        ]
        Resource = "*"
      },
      {
        Sid    = "GlueReadOnly"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartitions"
        ]
        Resource = "*"
      },
      {
        Sid    = "S3ReadData"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [
          "arn:aws:s3:::pb-silver-brasilmart-${var.account_id}",
          "arn:aws:s3:::pb-silver-brasilmart-${var.account_id}/*",
          "arn:aws:s3:::pb-gold-brasilmart-${var.account_id}",
          "arn:aws:s3:::pb-gold-brasilmart-${var.account_id}/*"
        ]
      },
      {
        Sid    = "AthenaResults"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::aws-athena-query-results-${var.account_id}-${var.aws_region}",
          "arn:aws:s3:::aws-athena-query-results-${var.account_id}-${var.aws_region}/*"
        ]
      },
      {
        Sid      = "LakeFormationAccess"
        Effect   = "Allow"
        Action   = ["lakeformation:GetDataAccess"]
        Resource = "*"
      }
    ]
  })
}

# ---------------------------------------------------------------
# Lake Formation: DESCRIBE nos databases
# ---------------------------------------------------------------

resource "aws_lakeformation_permissions" "analista_jr_silver_db" {
  principal = aws_iam_role.analista_jr.arn

  permissions = ["DESCRIBE"]

  database {
    name = "pb_silver_brasilmart"
  }
}

resource "aws_lakeformation_permissions" "analista_jr_gold_db" {
  principal = aws_iam_role.analista_jr.arn

  permissions = ["DESCRIBE"]

  database {
    name = "pb_gold_brasilmart"
  }
}

resource "aws_lakeformation_permissions" "analista_jr_bronze_db" {
  principal = aws_iam_role.analista_jr.arn

  permissions = ["DESCRIBE"]

  database {
    name = "pb_bronze_brasilmart"
  }
}

# ---------------------------------------------------------------
# Column-Level: customers — EXCLUIR customer_id, customer_unique_id
# ---------------------------------------------------------------

resource "aws_lakeformation_permissions" "analista_jr_customers_bronze" {
  principal   = aws_iam_role.analista_jr.arn
  permissions = ["SELECT"]

  table_with_columns {
    database_name = "pb_bronze_brasilmart"
    name          = "customers"
    excluded_column_names = [
      "customer_id",
      "customer_unique_id"
    ]
    wildcard = true
  }
}

resource "aws_lakeformation_permissions" "analista_jr_customers_silver" {
  principal   = aws_iam_role.analista_jr.arn
  permissions = ["SELECT"]

  table_with_columns {
    database_name = "pb_silver_brasilmart"
    name          = "customers"
    excluded_column_names = [
      "customer_id",
      "customer_unique_id"
    ]
    wildcard = true
  }
}

# ---------------------------------------------------------------
# Column-Level: sellers — EXCLUIR seller_id
# ---------------------------------------------------------------

resource "aws_lakeformation_permissions" "analista_jr_sellers_bronze" {
  principal   = aws_iam_role.analista_jr.arn
  permissions = ["SELECT"]

  table_with_columns {
    database_name = "pb_bronze_brasilmart"
    name          = "sellers"
    excluded_column_names = ["seller_id"]
    wildcard = true
  }
}

resource "aws_lakeformation_permissions" "analista_jr_sellers_silver" {
  principal   = aws_iam_role.analista_jr.arn
  permissions = ["SELECT"]

  table_with_columns {
    database_name = "pb_silver_brasilmart"
    name          = "sellers"
    excluded_column_names = ["seller_id"]
    wildcard = true
  }
}

# ---------------------------------------------------------------
# Column-Level: geolocation — EXCLUIR lat/lng (coordenadas GPS)
# ---------------------------------------------------------------

resource "aws_lakeformation_permissions" "analista_jr_geolocation" {
  principal   = aws_iam_role.analista_jr.arn
  permissions = ["SELECT"]

  table_with_columns {
    database_name = "pb_bronze_brasilmart"
    name          = "geolocation"
    excluded_column_names = [
      "geolocation_lat",
      "geolocation_lng"
    ]
    wildcard = true
  }
}

# ---------------------------------------------------------------
# Column-Level: reviews — EXCLUIR texto livre (pode conter PII)
# ---------------------------------------------------------------

resource "aws_lakeformation_permissions" "analista_jr_reviews" {
  principal   = aws_iam_role.analista_jr.arn
  permissions = ["SELECT"]

  table_with_columns {
    database_name = "pb_bronze_brasilmart"
    name          = "reviews"
    excluded_column_names = [
      "review_comment_title",
      "review_comment_message"
    ]
    wildcard = true
  }
}

# ---------------------------------------------------------------
# Full access: orders, items, payments, products (sem PII direto)
# ---------------------------------------------------------------

resource "aws_lakeformation_permissions" "analista_jr_orders" {
  principal   = aws_iam_role.analista_jr.arn
  permissions = ["SELECT", "DESCRIBE"]

  table {
    database_name = "pb_bronze_brasilmart"
    name          = "orders"
  }
}

resource "aws_lakeformation_permissions" "analista_jr_items" {
  principal   = aws_iam_role.analista_jr.arn
  permissions = ["SELECT", "DESCRIBE"]

  table {
    database_name = "pb_bronze_brasilmart"
    name          = "items"
  }
}

resource "aws_lakeformation_permissions" "analista_jr_payments" {
  principal   = aws_iam_role.analista_jr.arn
  permissions = ["SELECT", "DESCRIBE"]

  table {
    database_name = "pb_bronze_brasilmart"
    name          = "payments"
  }
}

resource "aws_lakeformation_permissions" "analista_jr_products" {
  principal   = aws_iam_role.analista_jr.arn
  permissions = ["SELECT", "DESCRIBE"]

  table {
    database_name = "pb_bronze_brasilmart"
    name          = "products"
  }
}

# ---------------------------------------------------------------
# Gold: acesso completo (dados agregados, sem PII individual)
# ---------------------------------------------------------------

resource "aws_lakeformation_permissions" "analista_jr_gold_all" {
  principal   = aws_iam_role.analista_jr.arn
  permissions = ["SELECT", "DESCRIBE"]

  table {
    database_name = "pb_gold_brasilmart"
    name          = "ALL_TABLES"
    wildcard      = true
  }
}
