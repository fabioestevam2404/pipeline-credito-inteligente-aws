# ============================================================
# terraform/modules/stepfunctions/main.tf
# State Machine do pipeline + agendamento EventBridge
# ============================================================

resource "aws_sfn_state_machine" "credit_pipeline" {
  name     = "credit-pipeline-${var.environment}"
  role_arn = var.stepfunctions_role_arn
  type     = "STANDARD"

  # Logging para debug em caso de falha
  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_logs.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  definition = jsonencode({
    Comment = "Pipeline de Crédito AWS — Orquestração Completa v1.0"
    StartAt = "IngestCRM"
    States = {

      IngestCRM = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync:2"
        Parameters = {
          JobName = var.glue_job_ingest_crm
          Arguments = {
            "--run_date.$" = "$.run_date"
          }
        }
        ResultPath = "$.crm_result"
        Retry = [{
          ErrorEquals     = ["Glue.AWSGlueException", "States.TaskFailed"]
          IntervalSeconds = 60
          MaxAttempts     = 3
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        Next = "IngestBureau"
      }

      IngestBureau = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = "${var.lambda_bureau_batch_arn}:$LATEST"
          "Payload.$"  = "$"
        }
        ResultPath = "$.bureau_result"
        Retry = [{
          ErrorEquals     = ["Lambda.ServiceException", "Lambda.TooManyRequestsException"]
          IntervalSeconds = 30
          MaxAttempts     = 2
          BackoffRate     = 1.5
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        Next = "RunSilverProcessing"
      }

      RunSilverProcessing = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync:2"
        Parameters = {
          JobName = var.glue_job_silver_processing
          Arguments = {
            "--run_date.$" = "$.run_date"
          }
        }
        ResultPath = "$.silver_result"
        Retry = [{
          ErrorEquals     = ["Glue.AWSGlueException"]
          IntervalSeconds = 120
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        Next = "RunDataQuality"
      }

      RunDataQuality = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync:2"
        Parameters = {
          JobName = "credit-data-quality"
          Arguments = {
            "--run_date.$" = "$.run_date"
          }
        }
        ResultPath = "$.dq_result"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyQualityFailure"
        }]
        Next = "RunFeatureEngineering"
      }

      RunFeatureEngineering = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync:2"
        Parameters = {
          JobName = var.glue_job_feature_engineering
          Arguments = {
            "--run_date.$" = "$.run_date"
          }
        }
        ResultPath = "$.features_result"
        Retry = [{
          ErrorEquals     = ["Glue.AWSGlueException"]
          IntervalSeconds = 60
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        Next = "IngestFeatureStore"
      }

      IngestFeatureStore = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = "${var.lambda_feature_store_arn}:$LATEST"
          "Payload.$"  = "$"
        }
        ResultPath = "$.fs_result"
        Retry = [{
          ErrorEquals     = ["Lambda.ServiceException"]
          IntervalSeconds = 30
          MaxAttempts     = 3
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        Next = "PublishPipelineMetrics"
      }

      PublishPipelineMetrics = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = "${var.lambda_metrics_arn}:$LATEST"
          "Payload.$"  = "$"
        }
        ResultPath = "$.metrics_result"
        Next       = "NotifySuccess"
      }

      NotifySuccess = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn  = var.sns_alerts_arn
          Subject   = "✅ Pipeline de Crédito — Sucesso"
          "Message.$" = "States.Format('Pipeline concluído com sucesso para data: {}', $.run_date)"
        }
        End = true
      }

      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = var.sns_alerts_arn
          Subject  = "🚨 ALERTA: Pipeline de Crédito Falhou"
          "Message.$" = "States.Format('Pipeline falhou na etapa: {}. Erro: {}', $.error.Cause, $.error.Error)"
        }
        Next = "PipelineFailed"
      }

      NotifyQualityFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = var.sns_alerts_arn
          Subject  = "⚠️ ALERTA: Data Quality Falhou — Pipeline Interrompido"
          Message  = "Regras de qualidade falharam. Pipeline interrompido para revisão manual. Verificar Glue Data Quality no Console."
        }
        Next = "PipelineFailed"
      }

      PipelineFailed = {
        Type  = "Fail"
        Error = "PipelineExecutionError"
        Cause = "Verificar CloudWatch Logs e Step Functions execution para detalhes"
      }
    }
  })

  tags = { Environment = var.environment, Team = "data-engineering" }
}

# ── CloudWatch Log Group para Step Functions ──────────────────
resource "aws_cloudwatch_log_group" "sfn_logs" {
  name              = "/aws/states/credit-pipeline-${var.environment}"
  retention_in_days = 30
}

# ── EventBridge — Agendamento Diário às 02h UTC ───────────────
resource "aws_cloudwatch_event_rule" "daily_trigger" {
  name                = "credit-pipeline-daily-trigger"
  description         = "Aciona o pipeline de crédito diariamente às 02:00 UTC"
  schedule_expression = "cron(0 2 * * ? *)"
  is_enabled          = true
}

resource "aws_cloudwatch_event_target" "sfn_target" {
  rule     = aws_cloudwatch_event_rule.daily_trigger.name
  arn      = aws_sfn_state_machine.credit_pipeline.id
  role_arn = var.eventbridge_role_arn

  # Injeta a data de execução como parâmetro para o pipeline
  input_transformer {
    input_paths = {
      execution_time = "$.time"
    }
    # Formata como YYYY-MM-DD para uso nos Glue Jobs
    input_template = <<-JSON
      {
        "run_date": "<execution_time>",
        "triggered_by": "EventBridge",
        "environment": "${var.environment}"
      }
    JSON
  }
}

# ── Outputs ──────────────────────────────────────────────────
output "state_machine_arn"  { value = aws_sfn_state_machine.credit_pipeline.arn }
output "state_machine_name" { value = aws_sfn_state_machine.credit_pipeline.name }
