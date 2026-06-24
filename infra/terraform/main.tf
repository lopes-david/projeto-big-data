terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket = "brasilmart-terraform-state"
    key    = "data-platform/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "BrasilMart-DataPlatform"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name prefix for resource naming"
  type        = string
  default     = "brasilmart"
}

variable "account_id" {
  description = "AWS Account ID"
  type        = string
  default     = "234828142988"
}

variable "codestar_connection_arn" {
  description = "ARN da CodeStar Connection para o GitHub"
  type        = string
  default     = ""
}

variable "alert_email" {
  description = "E-mail para alertas SNS"
  type        = string
  default     = "david.lopes@al.infnet.edu.br"
}
