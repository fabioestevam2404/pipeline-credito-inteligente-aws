terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
  backend "s3" {
    bucket         = "terraform-state-credit-pipeline"
    key            = "prod/pipeline.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "terraform-state-lock"
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "credit-pipeline"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}
locals { account_id = data.aws_caller_identity.current.account_id }

module "kms" {
  source      = "../../modules/kms"
  environment = var.environment
  account_id  = local.account_id
  aws_region  = var.aws_region
}

module "s3" {
  source         = "../../modules/s3"
  environment    = var.environment
  account_id     = local.account_id
  credit_key_arn = module.kms.credit_key_arn
  audit_key_arn  = module.kms.audit_key_arn
  depends_on     = [module.kms]
}

module "iam" {
  source                = "../../modules/iam"
  environment           = var.environment
  account_id            = local.account_id
  aws_region            = var.aws_region
  credit_key_arn        = module.kms.credit_key_arn
  raw_bucket_arn        = module.s3.raw_bucket_arn
  silver_bucket_arn     = module.s3.silver_bucket_arn
  gold_bucket_arn       = module.s3.gold_bucket_arn
  compliance_bucket_arn = "arn:aws:s3:::${module.s3.compliance_bucket_name}"
  artifacts_bucket_arn  = "arn:aws:s3:::${module.s3.artifacts_bucket_name}"
  depends_on            = [module.kms, module.s3]
}

module "glue" {
  source                = "../../modules/glue"
  environment           = var.environment
  account_id            = local.account_id
  aws_region            = var.aws_region
  glue_role_arn         = module.iam.glue_role_arn
  artifacts_bucket      = module.s3.artifacts_bucket_name
  raw_bucket            = module.s3.raw_bucket_name
  silver_bucket         = module.s3.silver_bucket_name
  gold_bucket           = module.s3.gold_bucket_name
  credit_key_arn        = module.kms.credit_key_arn
  crm_host              = var.crm_host
  crm_database          = var.crm_database
  private_subnet_id     = var.private_subnet_id
  depends_on            = [module.iam, module.s3]
}

module "lambda" {
  source            = "../../modules/lambda"
  environment       = var.environment
  account_id        = local.account_id
  aws_region        = var.aws_region
  lambda_role_arn   = module.iam.lambda_role_arn
  raw_bucket        = module.s3.raw_bucket_name
  compliance_bucket = module.s3.compliance_bucket_name
  credit_key_arn    = module.kms.credit_key_arn
  alert_topic_arn   = "placeholder"  # Substituído após criação do SNS
  depends_on        = [module.iam, module.s3]
}

module "step_functions" {
  source                       = "../../modules/step_functions"
  environment                  = var.environment
  account_id                   = local.account_id
  aws_region                   = var.aws_region
  stepfunctions_role_arn       = module.iam.stepfunctions_role_arn
  sagemaker_role_arn           = module.iam.sagemaker_role_arn
  ingest_crm_job_name          = module.glue.ingest_crm_job_name
  silver_processing_job_name   = module.glue.silver_processing_job_name
  feature_engineering_job_name = module.glue.feature_engineering_job_name
  bureau_lambda_arn            = module.lambda.credit_score_function
  gold_bucket                  = module.s3.gold_bucket_name
  artifacts_bucket             = module.s3.artifacts_bucket_name
  alert_email                  = var.alert_email
  depends_on                   = [module.glue, module.lambda]
}

module "monitoring" {
  source          = "../../modules/monitoring"
  environment     = var.environment
  account_id      = local.account_id
  aws_region      = var.aws_region
  alert_topic_arn = module.step_functions.alert_topic_arn
  score_endpoint  = "credit-score-model-v1-${var.environment}"
  audit_key_arn   = module.kms.audit_key_arn
  depends_on      = [module.step_functions]
}

output "score_api_url"      { value = module.lambda.score_api_url }
output "state_machine_arn"  { value = module.step_functions.state_machine_arn }
