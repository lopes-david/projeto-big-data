# ============================================================
# AWS Lake Formation — Governança e Controle de Acesso
# ============================================================

# IAM Role para Lake Formation administrar os recursos
resource "aws_iam_role" "lake_formation_admin" {
  name = "${var.project_name}-lake-formation-admin"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lakeformation.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "lake_formation_admin" {
  name = "${var.project_name}-lake-formation-policy"
  role = aws_iam_role.lake_formation_admin.id

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
      }
    ]
  })
}

# Registrar Lake Formation como administrador do Data Lake
resource "aws_lakeformation_data_lake_settings" "settings" {
  admins = [aws_iam_role.lake_formation_admin.arn]
}

# Registrar os buckets S3 como locations do Data Lake
resource "aws_lakeformation_resource" "buckets" {
  for_each = aws_s3_bucket.lakehouse
  arn      = each.value.arn
  role_arn = aws_iam_role.lake_formation_admin.arn
}

# IAM Role para analistas de dados (acesso restrito)
resource "aws_iam_role" "data_analyst" {
  name = "${var.project_name}-data-analyst"

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

# Permissão Lake Formation: analistas só acessam Silver e Gold
resource "aws_lakeformation_permissions" "analyst_silver" {
  principal = aws_iam_role.data_analyst.arn

  permissions = ["SELECT", "DESCRIBE"]

  database {
    name = aws_glue_catalog_database.silver.name
  }
}

resource "aws_lakeformation_permissions" "analyst_gold" {
  principal = aws_iam_role.data_analyst.arn

  permissions = ["SELECT", "DESCRIBE"]

  database {
    name = aws_glue_catalog_database.gold.name
  }
}
