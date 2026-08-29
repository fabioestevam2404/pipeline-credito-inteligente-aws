# ─── Monitoring Module ─────────────────────────────────────────────────────────

variable "environment"       {}
variable "account_id"        {}
variable "aws_region"        {}
variable "alert_topic_arn"   {}
variable "score_endpoint"    {}
variable "audit_key_arn"     {}

# ── Alarmes CloudWatch ────────────────────────────────────────────────────────

# SageMaker: latência p99 > 300ms
resource "aws_cloudwatch_metric_alarm" "score_latency_p99" {
  alarm_name          = "credit-score-latency-p99-${var.environment}"
  alarm_description   = "Score endpoint P99 latency > 300ms — possível degradação"
  namespace           = "AWS/SageMaker"
  metric_name         = "ModelLatency"
  dimensions          = { EndpointName = var.score_endpoint }
  statistic           = "p99"
  period              = 60
  evaluation_periods  = 5
  threshold           = 300000  # microsegundos
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.alert_topic_arn]
  ok_actions          = [var.alert_topic_arn]
}

# Lambda: taxa de erro > 5%
resource "aws_cloudwatch_metric_alarm" "lambda_error_rate" {
  alarm_name          = "credit-lambda-error-rate-${var.environment}"
  alarm_description   = "Lambda credit-score error rate > 5%"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = "credit-score-${var.environment}" }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 3
  threshold           = 10
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.alert_topic_arn]
}

# Glue Job: falhas
resource "aws_cloudwatch_metric_alarm" "glue_failures" {
  alarm_name          = "credit-glue-job-failures-${var.environment}"
  alarm_description   = "Glue job falhou — pipeline de crédito"
  namespace           = "Glue"
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  dimensions          = { JobName = "credit-silver-processing-${var.environment}" }
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.alert_topic_arn]
}

# Volume baixo de ingestão (indica problema na fonte)
resource "aws_cloudwatch_metric_alarm" "low_ingestion" {
  alarm_name          = "credit-low-ingestion-volume-${var.environment}"
  alarm_description   = "Volume de ingestão abaixo do esperado — verificar fonte CRM"
  namespace           = "CreditPipeline/Custom"
  metric_name         = "RecordsIngested"
  statistic           = "Sum"
  period              = 86400
  evaluation_periods  = 1
  threshold           = 1000
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [var.alert_topic_arn]
}

# Data Quality: taxa de registros descartados > 10%
resource "aws_cloudwatch_metric_alarm" "high_drop_rate" {
  alarm_name          = "credit-high-data-drop-rate-${var.environment}"
  alarm_description   = "Taxa de descarte > 10% — possível degradação da qualidade na fonte"
  namespace           = "CreditPipeline/Custom"
  metric_name         = "DataDropRate"
  statistic           = "Average"
  period              = 86400
  evaluation_periods  = 1
  threshold           = 10
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.alert_topic_arn]
}

# ── CloudWatch Dashboard ──────────────────────────────────────────────────────
resource "aws_cloudwatch_dashboard" "credit_pipeline" {
  dashboard_name = "CreditPipeline-${var.environment}"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "text"
        x = 0, y = 0, width = 24, height = 1
        properties = {
          markdown = "# Pipeline de Crédito — ${upper(var.environment)} | Atualizado em tempo real"
        }
      },
      {
        type = "metric"
        x = 0, y = 1, width = 8, height = 6
        properties = {
          title  = "Score Endpoint — Latência P99"
          metrics = [[
            "AWS/SageMaker", "ModelLatency",
            "EndpointName", var.score_endpoint,
            { stat = "p99", period = 60 }
          ]]
          view   = "timeSeries"
          yAxis  = { left = { label = "Microsegundos", min = 0 } }
        }
      },
      {
        type = "metric"
        x = 8, y = 1, width = 8, height = 6
        properties = {
          title  = "Ingestão — Registros por Execução"
          metrics = [
            ["CreditPipeline/Custom", "RecordsIngested",   { stat = "Sum", period = 86400, label = "Raw" }],
            ["CreditPipeline/Custom", "RecordsProcessed",  { stat = "Sum", period = 86400, label = "Silver" }],
            ["CreditPipeline/Custom", "RecordsDropped",    { stat = "Sum", period = 86400, label = "Descartados" }]
          ]
          view = "timeSeries"
        }
      },
      {
        type = "metric"
        x = 16, y = 1, width = 8, height = 6
        properties = {
          title  = "Lambda Score — Erros e Invocações"
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", "credit-score-${var.environment}", { stat = "Sum" }],
            ["AWS/Lambda", "Errors",      "FunctionName", "credit-score-${var.environment}", { stat = "Sum", color = "#d62728" }]
          ]
          view = "timeSeries"
        }
      },
      {
        type = "alarm"
        x = 0, y = 7, width = 24, height = 3
        properties = {
          title  = "Status dos Alarmes"
          alarms = [
            "arn:aws:cloudwatch:${var.aws_region}:${var.account_id}:alarm:credit-score-latency-p99-${var.environment}",
            "arn:aws:cloudwatch:${var.aws_region}:${var.account_id}:alarm:credit-lambda-error-rate-${var.environment}",
            "arn:aws:cloudwatch:${var.aws_region}:${var.account_id}:alarm:credit-glue-job-failures-${var.environment}",
            "arn:aws:cloudwatch:${var.aws_region}:${var.account_id}:alarm:credit-low-ingestion-volume-${var.environment}"
          ]
        }
      }
    ]
  })
}

# ── DynamoDB: Tabela de Auditoria ─────────────────────────────────────────────
resource "aws_dynamodb_table" "audit" {
  name           = "credit-score-audit-${var.environment}"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "application_id"
  range_key      = "decision_ts"

  attribute {
    name = "application_id"
    type = "S"
  }

  attribute {
    name = "decision_ts"
    type = "S"
  }

  attribute {
    name = "cpf_hash"
    type = "S"
  }

  # GSI para buscar decisões por CPF (LGPD: direito de acesso)
  global_secondary_index {
    name            = "cpf-hash-index"
    hash_key        = "cpf_hash"
    range_key       = "decision_ts"
    projection_type = "ALL"
  }

  # TTL automático após 5 anos
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.audit_key_arn
  }

  tags = { Environment = var.environment, Compliance = "BCB-LGPD" }
}

output "audit_table_name" { value = aws_dynamodb_table.audit.name }
output "audit_table_arn"  { value = aws_dynamodb_table.audit.arn }
