# ============================================================
# terraform/environments/prod/main.tf
# Ponto de entrada — conecta todos os módulos
# ============================================================

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Backend S3 para estado remoto — crie o bucket antes do primeiro apply
  backend "s3" {
    bucket         = "credit-pipeline-tfstate"
    key            = "prod/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "credit-pipeline-tf-locks"   # Para lock de state
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "CreditPipeline"
      Environment = var.environment
      ManagedBy   = "Terraform"
      Team        = "data-engineering"
    }
  }
}

# ── Módulo KMS ────────────────────────────────────────────────
module "kms" {
  source      = "../../modules/kms"
  account_id  = var.account_id
  environment = var.environment
}

# ── Módulo S3 ─────────────────────────────────────────────────
module "s3" {
  source      = "../../modules/s3"
  account_id  = var.account_id
  environment = var.environment
  kms_key_arn = module.kms.key_arn
}

# ── Módulo IAM ────────────────────────────────────────────────
module "iam" {
  source      = "../../modules/iam"
  account_id  = var.account_id
  environment = var.environment
  kms_key_arn = module.kms.key_arn
}

# ── Módulo Monitoramento ──────────────────────────────────────
module "monitoring" {
  source      = "../../modules/monitoring"
  account_id  = var.account_id
  environment = var.environment
  aws_region  = var.aws_region
  kms_key_arn = module.kms.key_arn
  alert_email = var.alert_email
}

# ── Módulo Glue ───────────────────────────────────────────────
module "glue" {
  source               = "../../modules/glue"
  environment          = var.environment
  aws_region           = var.aws_region
  glue_role_arn        = module.iam.glue_role_arn
  sagemaker_role_arn   = module.iam.sagemaker_role_arn
  kms_key_arn          = module.kms.key_arn
  raw_bucket           = module.s3.raw_bucket_name
  silver_bucket        = module.s3.silver_bucket_name
  gold_bucket          = module.s3.gold_bucket_name
  scripts_bucket       = module.s3.raw_bucket_name   # Scripts ficam no raw bucket
  crm_host             = var.crm_host
  crm_username         = var.crm_username
  crm_password         = var.crm_password
  private_subnet_id    = var.private_subnet_id
  glue_security_group_id = var.glue_security_group_id
}

# ── Módulo Step Functions ─────────────────────────────────────
module "stepfunctions" {
  source                    = "../../modules/stepfunctions"
  environment               = var.environment
  stepfunctions_role_arn    = module.iam.stepfunctions_role_arn
  eventbridge_role_arn      = module.iam.eventbridge_role_arn
  sns_alerts_arn            = module.monitoring.sns_alerts_arn
  glue_job_ingest_crm       = module.glue.job_ingest_crm_name
  glue_job_silver_processing= module.glue.job_silver_processing_name
  glue_job_feature_engineering = module.glue.job_feature_engineering_name
  lambda_bureau_batch_arn   = aws_lambda_function.bureau_batch.arn
  lambda_feature_store_arn  = aws_lambda_function.feature_store_ingest.arn
  lambda_metrics_arn        = aws_lambda_function.metrics_publisher.arn
}

# ── Lambda: Bureau Batch Query ────────────────────────────────
resource "aws_lambda_function" "bureau_batch" {
  function_name = "credit-bureau-batch-query"
  role          = module.iam.lambda_role_arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.11"
  timeout       = 300   # 5 minutos para batch
  memory_size   = 512

  filename         = "${path.module}/../../../dist/bureau_batch.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../dist/bureau_batch.zip")

  environment {
    variables = {
      RAW_BUCKET   = module.s3.raw_bucket_name
      ENVIRONMENT  = var.environment
      KMS_KEY_ARN  = module.kms.key_arn
    }
  }
  kms_key_arn = module.kms.key_arn
}

# ── Lambda: Score Endpoint ─────────────────────────────────────
resource "aws_lambda_function" "score_endpoint" {
  function_name = "credit-score-endpoint"
  role          = module.iam.lambda_role_arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.11"
  timeout       = 30
  memory_size   = 256

  filename         = "${path.module}/../../../dist/score_endpoint.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../dist/score_endpoint.zip")

  environment {
    variables = {
      ENDPOINT_NAME  = "credit-score-model-v1"
      FEATURE_GROUP  = "credit-features-v1"
      AUDIT_TABLE    = module.monitoring.dynamodb_audit_name
      ENVIRONMENT    = var.environment
    }
  }
  kms_key_arn = module.kms.key_arn
}

# ── Lambda: Feature Store Ingest ──────────────────────────────
resource "aws_lambda_function" "feature_store_ingest" {
  function_name = "credit-feature-store-ingest"
  role          = module.iam.lambda_role_arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.11"
  timeout       = 600   # 10 min para ingestão em batch
  memory_size   = 1024

  filename         = "${path.module}/../../../dist/feature_store_ingest.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../dist/feature_store_ingest.zip")

  environment {
    variables = {
      GOLD_BUCKET   = module.s3.gold_bucket_name
      FEATURE_GROUP = "credit-features-v1"
    }
  }
}

# ── Lambda: Metrics Publisher ─────────────────────────────────
resource "aws_lambda_function" "metrics_publisher" {
  function_name = "credit-metrics-publisher"
  role          = module.iam.lambda_role_arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.11"
  timeout       = 60
  memory_size   = 128

  filename         = "${path.module}/../../../dist/metrics_publisher.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../dist/metrics_publisher.zip")
}

# ── API Gateway para o endpoint de Score ──────────────────────
resource "aws_api_gateway_rest_api" "score_api" {
  name        = "credit-score-api-${var.environment}"
  description = "API REST para consulta de score de crédito"
}

resource "aws_api_gateway_resource" "score_resource" {
  rest_api_id = aws_api_gateway_rest_api.score_api.id
  parent_id   = aws_api_gateway_rest_api.score_api.root_resource_id
  path_part   = "score"
}

resource "aws_api_gateway_method" "score_post" {
  rest_api_id   = aws_api_gateway_rest_api.score_api.id
  resource_id   = aws_api_gateway_resource.score_resource.id
  http_method   = "POST"
  authorization = "AWS_IAM"   # Autenticação via IAM — não usar NONE em produção
}

resource "aws_api_gateway_integration" "lambda_integration" {
  rest_api_id             = aws_api_gateway_rest_api.score_api.id
  resource_id             = aws_api_gateway_resource.score_resource.id
  http_method             = aws_api_gateway_method.score_post.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.score_endpoint.invoke_arn
}

# ── Outputs Gerais ────────────────────────────────────────────
output "raw_bucket"         { value = module.s3.raw_bucket_name }
output "silver_bucket"      { value = module.s3.silver_bucket_name }
output "gold_bucket"        { value = module.s3.gold_bucket_name }
output "kms_key_arn"        { value = module.kms.key_arn }
output "state_machine_arn"  { value = module.stepfunctions.state_machine_arn }
output "score_api_endpoint" {
  value = "https://${aws_api_gateway_rest_api.score_api.id}.execute-api.${var.aws_region}.amazonaws.com/prod/score"
}
