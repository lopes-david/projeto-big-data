# TP4 1.1 — Recursos CI/CD: CodePipeline + CodeBuild para deploy de dbt e infra

resource "aws_s3_bucket" "codepipeline_artifacts" {
  bucket = "pb-brasilmart-codepipeline-artifacts-${var.account_id}"

  tags = {
    Name = "CodePipeline Artifacts"
    TP   = "tp4"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "codepipeline_artifacts" {
  bucket = aws_s3_bucket.codepipeline_artifacts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "codepipeline_artifacts" {
  bucket                  = aws_s3_bucket.codepipeline_artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# IAM Role para CodePipeline
resource "aws_iam_role" "codepipeline_role" {
  name = "CodePipelineServiceRole-pb-brasilmart"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "codepipeline.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = { TP = "tp4" }
}

resource "aws_iam_role_policy" "codepipeline_policy" {
  name = "CodePipelinePolicy-pb-brasilmart"
  role = aws_iam_role.codepipeline_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:GetBucketVersioning"
        ]
        Resource = [
          aws_s3_bucket.codepipeline_artifacts.arn,
          "${aws_s3_bucket.codepipeline_artifacts.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "codebuild:BatchGetBuilds",
          "codebuild:StartBuild"
        ]
        Resource = [
          aws_codebuild_project.dbt_build.arn,
          aws_codebuild_project.infra_build.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "codestar-connections:UseConnection"
        ]
        Resource = "*"
      }
    ]
  })
}

# IAM Role para CodeBuild
resource "aws_iam_role" "codebuild_role" {
  name = "CodeBuildServiceRole-pb-brasilmart"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "codebuild.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = { TP = "tp4" }
}

resource "aws_iam_role_policy" "codebuild_policy" {
  name = "CodeBuildPolicy-pb-brasilmart"
  role = aws_iam_role.codebuild_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${var.account_id}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ]
        Resource = "${aws_s3_bucket.codepipeline_artifacts.arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:${var.account_id}:secret:pb-brasilmart/*"
      },
      {
        Effect   = "Allow"
        Action   = ["redshift-data:*", "redshift-serverless:GetCredentials"]
        Resource = "*"
      }
    ]
  })
}

# CodeBuild — dbt
resource "aws_codebuild_project" "dbt_build" {
  name         = "pb-brasilmart-dbt-build"
  description  = "TP4 — Build e deploy do projeto dbt (staging + marts) no Redshift"
  service_role = aws_iam_role.codebuild_role.arn

  artifacts {
    type = "CODEPIPELINE"
  }

  environment {
    compute_type    = "BUILD_GENERAL1_SMALL"
    image           = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
    type            = "LINUX_CONTAINER"
    privileged_mode = false
  }

  source {
    type      = "CODEPIPELINE"
    buildspec = "infra/aws/codepipeline/buildspec_dbt.yml"
  }

  tags = { TP = "tp4" }
}

# CodeBuild — Infra (Terraform)
resource "aws_codebuild_project" "infra_build" {
  name         = "pb-brasilmart-infra-build"
  description  = "TP4 — Deploy de infraestrutura AWS via Terraform"
  service_role = aws_iam_role.codebuild_role.arn

  artifacts {
    type = "CODEPIPELINE"
  }

  environment {
    compute_type    = "BUILD_GENERAL1_SMALL"
    image           = "aws/codebuild/amazonlinux2-x86_64-standard:5.0"
    type            = "LINUX_CONTAINER"
    privileged_mode = false
  }

  source {
    type      = "CODEPIPELINE"
    buildspec = "infra/aws/codepipeline/buildspec_infra.yml"
  }

  tags = { TP = "tp4" }
}

# CodePipeline
resource "aws_codepipeline" "brasilmart_cicd" {
  name     = "pb-brasilmart-cicd"
  role_arn = aws_iam_role.codepipeline_role.arn

  artifact_store {
    location = aws_s3_bucket.codepipeline_artifacts.bucket
    type     = "S3"
  }

  stage {
    name = "Source"

    action {
      name             = "GitHub_Source"
      category         = "Source"
      owner            = "AWS"
      provider         = "CodeStarSourceConnection"
      version          = "1"
      output_artifacts = ["SourceArtifact"]

      configuration = {
        ConnectionArn    = var.codestar_connection_arn
        FullRepositoryId = "lopes-david/tp1"
        BranchName       = "main"
        DetectChanges    = "true"
      }
    }
  }

  stage {
    name = "Deploy_Infra"

    action {
      name            = "Terraform_Apply"
      category        = "Build"
      owner           = "AWS"
      provider        = "CodeBuild"
      version         = "1"
      input_artifacts = ["SourceArtifact"]

      configuration = {
        ProjectName = aws_codebuild_project.infra_build.name
      }
    }
  }

  stage {
    name = "Build_dbt"

    action {
      name            = "dbt_Run_Test"
      category        = "Build"
      owner           = "AWS"
      provider        = "CodeBuild"
      version         = "1"
      input_artifacts = ["SourceArtifact"]

      configuration = {
        ProjectName = aws_codebuild_project.dbt_build.name
      }
    }
  }

  tags = { TP = "tp4" }
}
