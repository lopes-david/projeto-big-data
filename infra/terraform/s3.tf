# ============================================================
# S3 Buckets — Data Lakehouse Layers (Raw, Bronze, Silver, Gold)
# ============================================================

locals {
  buckets = {
    raw = {
      name = "${var.project_name}-raw-${var.environment}"
      lifecycle_rules = [
        {
          id                     = "raw-to-ia"
          transition_days        = 90
          transition_class       = "STANDARD_IA"
          glacier_transition_days = 365
          expiration_days        = 2555 # 7 anos (requisito regulatório e-commerce/LGPD)
        }
      ]
    }
    bronze = {
      name = "${var.project_name}-bronze-${var.environment}"
      lifecycle_rules = [
        {
          id                     = "bronze-to-ia"
          transition_days        = 90
          transition_class       = "STANDARD_IA"
          glacier_transition_days = 730
          expiration_days        = 1095 # 3 anos
        }
      ]
    }
    silver = {
      name = "${var.project_name}-silver-${var.environment}"
      lifecycle_rules = [
        {
          id                     = "silver-to-ia"
          transition_days        = 180
          transition_class       = "STANDARD_IA"
          glacier_transition_days = null
          expiration_days        = 730 # 2 anos
        }
      ]
    }
    gold = {
      name = "${var.project_name}-gold-${var.environment}"
      lifecycle_rules = [
        {
          id                     = "gold-cleanup"
          transition_days        = null
          transition_class       = null
          glacier_transition_days = null
          expiration_days        = 365 # 1 ano (dados vivos, re-processados)
        }
      ]
    }
  }
}

resource "aws_s3_bucket" "lakehouse" {
  for_each = local.buckets
  bucket   = each.value.name
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.lakehouse[each.key].id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.lakehouse[each.key].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data_platform.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.lakehouse[each.key].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.lakehouse[each.key].id

  dynamic "rule" {
    for_each = each.value.lifecycle_rules
    content {
      id     = rule.value.id
      status = "Enabled"

      filter {
        prefix = ""
      }

      dynamic "transition" {
        for_each = rule.value.transition_days != null ? [1] : []
        content {
          days          = rule.value.transition_days
          storage_class = rule.value.transition_class
        }
      }

      dynamic "transition" {
        for_each = rule.value.glacier_transition_days != null ? [1] : []
        content {
          days          = rule.value.glacier_transition_days
          storage_class = "GLACIER"
        }
      }

      expiration {
        days = rule.value.expiration_days
      }
    }
  }
}

# KMS Key for S3 encryption
resource "aws_kms_key" "data_platform" {
  description             = "KMS key for BrasilMart Data Platform encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "data_platform" {
  name          = "alias/${var.project_name}-data-platform"
  target_key_id = aws_kms_key.data_platform.key_id
}

# Outputs
output "bucket_arns" {
  value = { for k, v in aws_s3_bucket.lakehouse : k => v.arn }
}

output "bucket_names" {
  value = { for k, v in aws_s3_bucket.lakehouse : k => v.bucket }
}
