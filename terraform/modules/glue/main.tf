# ─── Glue Module ──────────────────────────────────────────────────────────────
# Jobs ETL, Data Catalog e triggers

variable "environment"         {}
variable "account_id"          {}
variable "aws_region"          {}
variable "glue_role_arn"       {}
variable "artifacts_bucket"    {}
variable "raw_bucket"          {}
variable "silver_bucket"       {}
variable "gold_bucket"         {}
variable "credit_key_arn"      {}

# ── Glue Database ─────────────────────────────────────────────────────────────
resource "aws_glue_catalog_database" "credit_db" {
  name        = "credit_db_${var.environment}"
  description = "Database do pipeline de crédito — ${var.environment}"
}

# ── Security Configuration ────────────────────────────────────────────────────
resource "aws_glue_security_configuration" "credit_sec" {
  name = "credit-security-${var.environment}"

  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "SSE-KMS"
      kms_key_arn                = var.credit_key_arn
    }
    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "CSE-KMS"
      kms_key_arn                   = var.credit_key_arn
    }
    s3_encryption {
      s3_encryption_mode = "SSE-KMS"
      kms_key_arn        = var.credit_key_arn
    }
  }
}

# ── Glue Connection (JDBC para o CRM) ────────────────────────────────────────
resource "aws_glue_connection" "crm_connection" {
  name = "credit-crm-jdbc-${var.environment}"

  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:postgresql://${var.crm_host}:5432/${var.crm_database}"
    USERNAME            = "glue_reader"
    PASSWORD            = "{{resolve:secretsmanager:credit-pipeline/crm-db:SecretString:password}}"
  }

  physical_connection_requirements {
    availability_zone      = "${var.aws_region}a"
    security_group_id_list = [aws_security_group.glue_sg.id]
    subnet_id              = var.private_subnet_id
  }
}

# ── Job: Ingestão CRM ─────────────────────────────────────────────────────────
resource "aws_glue_job" "ingest_crm" {
  name              = "credit-ingest-crm-${var.environment}"
  role_arn          = var.glue_role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 5
  max_retries       = 2
  timeout           = 60  # minutos

  command {
    script_location = "s3://${var.artifacts_bucket}/glue-scripts/ingest_crm.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${var.artifacts_bucket}/spark-logs/"
    "--TempDir"                          = "s3://${var.artifacts_bucket}/glue-temp/"
    "--RAW_BUCKET"                       = var.raw_bucket
    "--ENVIRONMENT"                      = var.environment
    "--additional-python-modules"        = "boto3>=1.26.0"
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  }

  security_configuration = aws_glue_security_configuration.credit_sec.name

  tags = { Environment = var.environment, Component = "ingestion" }
}

# ── Job: Silver Processing ────────────────────────────────────────────────────
resource "aws_glue_job" "silver_processing" {
  name              = "credit-silver-processing-${var.environment}"
  role_arn          = var.glue_role_arn
  glue_version      = "4.0"
  worker_type       = "G.2X"
  number_of_workers = 10
  max_retries       = 1
  timeout           = 120

  command {
    script_location = "s3://${var.artifacts_bucket}/glue-scripts/silver_processing.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://${var.artifacts_bucket}/glue-temp/"
    "--RAW_BUCKET"                       = var.raw_bucket
    "--SILVER_BUCKET"                    = var.silver_bucket
    "--ENVIRONMENT"                      = var.environment
    "--datalake-formats"                 = "iceberg"
    "--conf" = join(" --conf ", [
      "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "spark.sql.catalog.glue_catalog=org.apache.iceberg.aws.glue.GlueCatalog",
      "spark.sql.catalog.glue_catalog.warehouse=s3://${var.silver_bucket}/"
    ])
  }

  security_configuration = aws_glue_security_configuration.credit_sec.name
}

# ── Job: Feature Engineering ──────────────────────────────────────────────────
resource "aws_glue_job" "feature_engineering" {
  name              = "credit-feature-engineering-${var.environment}"
  role_arn          = var.glue_role_arn
  glue_version      = "4.0"
  worker_type       = "G.2X"
  number_of_workers = 10
  timeout           = 90

  command {
    script_location = "s3://${var.artifacts_bucket}/glue-scripts/feature_engineering.py"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://${var.artifacts_bucket}/glue-temp/"
    "--SILVER_BUCKET"                    = var.silver_bucket
    "--GOLD_BUCKET"                      = var.gold_bucket
    "--ENVIRONMENT"                      = var.environment
    "--additional-python-modules"        = "sagemaker>=2.160.0"
  }

  security_configuration = aws_glue_security_configuration.credit_sec.name
}

# ── Glue Crawler (cataloga automaticamente novos dados) ───────────────────────
resource "aws_glue_crawler" "silver_crawler" {
  name          = "credit-silver-crawler-${var.environment}"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.credit_db.name
  schedule      = "cron(0 6 * * ? *)"  # Roda após o pipeline (às 6h)

  s3_target {
    path = "s3://${var.silver_bucket}/clientes/"
  }

  s3_target {
    path = "s3://${var.silver_bucket}/bureau/"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  schema_change_policy {
    delete_behavior = "LOG"        # Não deleta tabelas ao mudar schema
    update_behavior = "UPDATE_IN_DATABASE"
  }
}

output "ingest_crm_job_name"        { value = aws_glue_job.ingest_crm.name }
output "silver_processing_job_name" { value = aws_glue_job.silver_processing.name }
output "feature_engineering_job_name" { value = aws_glue_job.feature_engineering.name }
output "glue_database_name"         { value = aws_glue_catalog_database.credit_db.name }
