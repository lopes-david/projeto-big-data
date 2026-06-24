# ============================================================
# AWS Glue Data Catalog — Databases e Crawler
# ============================================================

# Databases no Glue Data Catalog (um por camada)
resource "aws_glue_catalog_database" "raw" {
  name        = "${var.project_name}_raw"
  description = "Dados brutos originais (JSON, CSV) - BrasilMart E-commerce"
}

resource "aws_glue_catalog_database" "bronze" {
  name        = "${var.project_name}_bronze"
  description = "Dados convertidos para Parquet/Delta sem transformação - BrasilMart E-commerce"
}

resource "aws_glue_catalog_database" "silver" {
  name        = "${var.project_name}_silver"
  description = "Dados limpos, padronizados e deduplicados - BrasilMart E-commerce"
}

resource "aws_glue_catalog_database" "gold" {
  name        = "${var.project_name}_gold"
  description = "Tabelas analíticas, KPIs e visão 360° do cliente - BrasilMart E-commerce"
}

# IAM Role para Glue Jobs
resource "aws_iam_role" "glue_etl" {
  name = "${var.project_name}-glue-etl-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_etl.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "${var.project_name}-glue-s3-access"
  role = aws_iam_role.glue_etl.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = flatten([
          for bucket in aws_s3_bucket.lakehouse : [
            bucket.arn,
            "${bucket.arn}/*"
          ]
        ])
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey"
        ]
        Resource = [aws_kms_key.data_platform.arn]
      },
      {
        Effect = "Allow"
        Action = [
          "lakeformation:GetDataAccess"
        ]
        Resource = ["*"]
      }
    ]
  })
}

# Crawler para descobrir schemas automaticamente na camada Raw
resource "aws_glue_crawler" "raw_orders" {
  database_name = aws_glue_catalog_database.raw.name
  name          = "${var.project_name}-raw-orders-crawler"
  role          = aws_iam_role.glue_etl.arn

  s3_target {
    path = "s3://${aws_s3_bucket.lakehouse["raw"].bucket}/orders/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })
}

# ============================================================
# AWS Glue Job — Ingestão Batch (CSV → Parquet)
# ============================================================

resource "aws_glue_job" "batch_ingestion" {
  name     = "${var.project_name}-batch-ingestion-orders"
  role_arn = aws_iam_role.glue_etl.arn

  command {
    script_location = "s3://${aws_s3_bucket.lakehouse["raw"].bucket}/scripts/batch_ingestion.py"
    python_version  = "3"
    name            = "glueetl"
  }

  default_arguments = {
    "--job-language"               = "python"
    "--job-bookmark-option"        = "job-bookmark-enable"
    "--enable-metrics"             = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                    = "s3://${aws_s3_bucket.lakehouse["raw"].bucket}/tmp/"
    "--SOURCE_PATH"                = "s3://${aws_s3_bucket.lakehouse["raw"].bucket}/orders/"
    "--TARGET_PATH"                = "s3://${aws_s3_bucket.lakehouse["bronze"].bucket}/orders/"
    "--DATABASE_NAME"              = aws_glue_catalog_database.bronze.name
    "--TABLE_NAME"                 = "orders"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  max_retries       = 1
  timeout           = 60
}

# Schedule para rodar o job diariamente às 6h UTC
resource "aws_glue_trigger" "batch_daily" {
  name     = "${var.project_name}-batch-daily-trigger"
  type     = "SCHEDULED"
  schedule = "cron(0 6 * * ? *)"

  actions {
    job_name = aws_glue_job.batch_ingestion.name
  }
}
