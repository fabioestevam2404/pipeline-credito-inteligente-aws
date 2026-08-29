# ─── Step Functions Module ────────────────────────────────────────────────────

variable "environment"               {}
variable "account_id"                {}
variable "aws_region"                {}
variable "stepfunctions_role_arn"    {}
variable "sagemaker_role_arn"        {}
variable "ingest_crm_job_name"       {}
variable "silver_processing_job_name"{}
variable "feature_engineering_job_name" {}
variable "bureau_lambda_arn"         {}
variable "alert_email"               {}
variable "credit_key_arn"            {}
variable "gold_bucket"               {}
variable "artifacts_bucket"          {}

resource "aws_sns_topic" "alerts" {
  name              = "credit-pipeline-alerts-${var.environment}"
  kms_master_key_id = var.credit_key_arn
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_sfn_state_machine" "credit_pipeline" {
  name     = "credit-pipeline-${var.environment}"
  role_arn = var.stepfunctions_role_arn

  definition = jsonencode({
    Comment = "Pipeline de Crédito — Orquestração Completa v1.0"
    StartAt = "IngestCRM"

    States = {
      IngestCRM = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName   = var.ingest_crm_job_name
          Arguments = { "--run_date.$" = "$.run_date" }
        }
        Retry = [{
          ErrorEquals   = ["Glue.AWSGlueException", "States.TaskFailed"]
          IntervalSeconds = 60
          MaxAttempts   = 3
          BackoffRate   = 2.0
          JitterStrategy = "FULL"
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        ResultPath = "$.crm_result"
        Next       = "IngestBureau"
      }

      IngestBureau = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.bureau_lambda_arn
          "Payload.$"  = "$"
        }
        Retry = [{
          ErrorEquals   = ["Lambda.ServiceException", "Lambda.TooManyRequestsException"]
          IntervalSeconds = 30
          MaxAttempts   = 2
          BackoffRate   = 1.5
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        ResultPath = "$.bureau_result"
        Next       = "RunSilverProcessing"
      }

      RunSilverProcessing = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName   = var.silver_processing_job_name
          Arguments = { "--run_date.$" = "$.run_date" }
        }
        Retry = [{
          ErrorEquals   = ["Glue.AWSGlueException"]
          IntervalSeconds = 120
          MaxAttempts   = 2
          BackoffRate   = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        ResultPath = "$.silver_result"
        Next       = "EvaluateDataQuality"
      }

      EvaluateDataQuality = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName   = "credit-data-quality-${var.environment}"
          Arguments = { "--run_date.$" = "$.run_date" }
        }
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.dq_error"
          Next        = "NotifyQualityFailure"
        }]
        ResultPath = "$.dq_result"
        Next       = "RunFeatureEngineering"
      }

      RunFeatureEngineering = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName   = var.feature_engineering_job_name
          Arguments = { "--run_date.$" = "$.run_date" }
        }
        Retry = [{
          ErrorEquals   = ["Glue.AWSGlueException"]
          IntervalSeconds = 60
          MaxAttempts   = 2
          BackoffRate   = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        ResultPath = "$.feature_result"
        Next       = "IngestFeatureStore"
      }

      IngestFeatureStore = {
        Type     = "Task"
        Resource = "arn:aws:states:::sagemaker:createProcessingJob.sync"
        Parameters = {
          "ProcessingJobName.$" = "States.Format('feature-store-ingest-{}', $.run_date)"
          AppSpecification = {
            ImageUri = "763104351884.dkr.ecr.us-east-1.amazonaws.com/sklearn:1.2-1"
            ContainerEntrypoint = ["python3", "/opt/ml/processing/ingest_features.py"]
          }
          ProcessingInputs = [{
            InputName = "code"
            S3Input = {
              "S3Uri"          = "s3://${var.artifacts_bucket}/sagemaker-scripts/"
              "LocalPath"      = "/opt/ml/processing"
              "S3DataType"     = "S3Prefix"
              "S3InputMode"    = "File"
            }
          }]
          RoleArn = var.sagemaker_role_arn
          ProcessingResources = {
            ClusterConfig = {
              InstanceCount  = 1
              InstanceType   = "ml.m5.xlarge"
              VolumeSizeInGB = 20
            }
          }
        }
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        ResultPath = "$.feature_store_result"
        Next       = "NotifySuccess"
      }

      NotifySuccess = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn  = aws_sns_topic.alerts.arn
          "Subject" = "✅ Pipeline de Crédito — Sucesso"
          "Message.$" = "States.Format('Pipeline concluído com sucesso para a data: {}', $.run_date)"
        }
        End = true
      }

      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn  = aws_sns_topic.alerts.arn
          Subject   = "🚨 Pipeline de Crédito — FALHA"
          "Message.$" = "States.Format('Pipeline falhou na data: {}. Erro: {}', $.run_date, $.error)"
        }
        Next = "PipelineFailed"
      }

      NotifyQualityFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.alerts.arn
          Subject  = "⚠️ Data Quality — Pipeline Interrompido"
          Message  = "Data Quality falhou. Pipeline interrompido para revisão manual dos dados."
        }
        Next = "PipelineFailed"
      }

      PipelineFailed = {
        Type  = "Fail"
        Error = "PipelineExecutionError"
        Cause = "Verificar CloudWatch Logs e X-Ray para diagnóstico"
      }
    }
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_logs.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  tracing_configuration {
    enabled = true
  }

  tags = { Environment = var.environment }
}

resource "aws_cloudwatch_log_group" "sfn_logs" {
  name              = "/aws/states/credit-pipeline-${var.environment}"
  retention_in_days = 90
}

# ── EventBridge: Agendamento diário ──────────────────────────────────────────
resource "aws_cloudwatch_event_rule" "daily_pipeline" {
  name                = "credit-pipeline-daily-${var.environment}"
  description         = "Aciona pipeline de crédito diariamente às 02:00 UTC"
  schedule_expression = "cron(0 2 * * ? *)"
  state               = var.environment == "prod" ? "ENABLED" : "DISABLED"
}

resource "aws_cloudwatch_event_target" "sfn_target" {
  rule     = aws_cloudwatch_event_rule.daily_pipeline.name
  arn      = aws_sfn_state_machine.credit_pipeline.id
  role_arn = var.stepfunctions_role_arn

  input_transformer {
    input_paths    = { "time" = "$.time" }
    input_template = "{\"run_date\": \"<time>\"}"
  }
}

output "state_machine_arn"  { value = aws_sfn_state_machine.credit_pipeline.id }
output "alert_topic_arn"    { value = aws_sns_topic.alerts.arn }
