# ─── Lambda Module ────────────────────────────────────────────────────────────

variable "environment"          {}
variable "account_id"           {}
variable "aws_region"           {}
variable "lambda_role_arn"      {}
variable "raw_bucket"           {}
variable "compliance_bucket"    {}
variable "credit_key_arn"       {}
variable "alert_topic_arn"      {}

# ── Kinesis Stream ────────────────────────────────────────────────────────────
resource "aws_kinesis_stream" "transactions" {
  name             = "credit-transactions-${var.environment}"
  shard_count      = 4
  retention_period = 24

  encryption_type = "KMS"
  kms_key_id      = var.credit_key_arn

  tags = { Environment = var.environment }
}

# ── Lambda: Bureau Query ──────────────────────────────────────────────────────
resource "aws_lambda_function" "bureau_query" {
  function_name = "credit-bureau-query-${var.environment}"
  role          = var.lambda_role_arn
  runtime       = "python3.11"
  handler       = "handler.lambda_handler"
  timeout       = 30
  memory_size   = 256

  filename         = data.archive_file.bureau_zip.output_path
  source_code_hash = data.archive_file.bureau_zip.output_base64sha256

  environment {
    variables = {
      RAW_BUCKET    = var.raw_bucket
      KMS_KEY_ID    = var.credit_key_arn
      ENVIRONMENT   = var.environment
      BUREAU_SECRET = "credit-pipeline/bureau/serasa"
    }
  }

  tracing_config { mode = "Active" }

  dead_letter_config {
    target_arn = aws_sqs_queue.bureau_dlq.arn
  }

  reserved_concurrent_executions = 100
}

resource "aws_sqs_queue" "bureau_dlq" {
  name                      = "credit-bureau-dlq-${var.environment}"
  message_retention_seconds = 1209600
  kms_master_key_id         = var.credit_key_arn
}

data "archive_file" "bureau_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../../../lambda_functions/bureau_query"
  output_path = "/tmp/bureau_query.zip"
}

# ── Lambda: Kinesis Consumer ──────────────────────────────────────────────────
resource "aws_lambda_function" "kinesis_consumer" {
  function_name = "credit-kinesis-consumer-${var.environment}"
  role          = var.lambda_role_arn
  runtime       = "python3.11"
  handler       = "handler.lambda_handler"
  timeout       = 60
  memory_size   = 512

  filename         = data.archive_file.kinesis_zip.output_path
  source_code_hash = data.archive_file.kinesis_zip.output_base64sha256

  environment {
    variables = {
      RAW_BUCKET  = var.raw_bucket
      KMS_KEY_ID  = var.credit_key_arn
      ENVIRONMENT = var.environment
    }
  }

  tracing_config { mode = "Active" }
}

resource "aws_lambda_event_source_mapping" "kinesis_trigger" {
  event_source_arn                   = aws_kinesis_stream.transactions.arn
  function_name                      = aws_lambda_function.kinesis_consumer.arn
  starting_position                  = "TRIM_HORIZON"
  batch_size                         = 500
  maximum_batching_window_in_seconds = 30
  parallelization_factor             = 4
  bisect_batch_on_function_error     = true

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.kinesis_dlq.arn
    }
  }
}

resource "aws_sqs_queue" "kinesis_dlq" {
  name                      = "credit-kinesis-dlq-${var.environment}"
  message_retention_seconds = 1209600
  kms_master_key_id         = var.credit_key_arn
}

data "archive_file" "kinesis_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../../../lambda_functions/kinesis_consumer"
  output_path = "/tmp/kinesis_consumer.zip"
}

# ── Lambda: Credit Score ──────────────────────────────────────────────────────
resource "aws_lambda_function" "credit_score" {
  function_name = "credit-score-${var.environment}"
  role          = var.lambda_role_arn
  runtime       = "python3.11"
  handler       = "handler.lambda_handler"
  timeout       = 15
  memory_size   = 512

  filename         = data.archive_file.score_zip.output_path
  source_code_hash = data.archive_file.score_zip.output_base64sha256

  environment {
    variables = {
      FEATURE_GROUP     = "credit-features-v1"
      ENDPOINT_NAME     = "credit-score-model-v1-${var.environment}"
      AUDIT_TABLE       = "credit-score-audit-${var.environment}"
      COMPLIANCE_BUCKET = var.compliance_bucket
      ENVIRONMENT       = var.environment
    }
  }

  tracing_config { mode = "Active" }
  reserved_concurrent_executions = 500
}

data "archive_file" "score_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../../../lambda_functions/credit_score"
  output_path = "/tmp/credit_score.zip"
}

# ── API Gateway ───────────────────────────────────────────────────────────────
resource "aws_api_gateway_rest_api" "credit_api" {
  name = "credit-score-api-${var.environment}"
  endpoint_configuration { types = ["REGIONAL"] }
}

resource "aws_api_gateway_resource" "score" {
  rest_api_id = aws_api_gateway_rest_api.credit_api.id
  parent_id   = aws_api_gateway_rest_api.credit_api.root_resource_id
  path_part   = "score"
}

resource "aws_api_gateway_method" "score_post" {
  rest_api_id   = aws_api_gateway_rest_api.credit_api.id
  resource_id   = aws_api_gateway_resource.score.id
  http_method   = "POST"
  authorization = "AWS_IAM"
}

resource "aws_api_gateway_integration" "score_lambda" {
  rest_api_id             = aws_api_gateway_rest_api.credit_api.id
  resource_id             = aws_api_gateway_resource.score.id
  http_method             = aws_api_gateway_method.score_post.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.credit_score.invoke_arn
}

resource "aws_api_gateway_deployment" "prod" {
  rest_api_id = aws_api_gateway_rest_api.credit_api.id
  depends_on  = [aws_api_gateway_integration.score_lambda]
  lifecycle   { create_before_destroy = true }
}

resource "aws_api_gateway_stage" "main" {
  deployment_id        = aws_api_gateway_deployment.prod.id
  rest_api_id          = aws_api_gateway_rest_api.credit_api.id
  stage_name           = var.environment
  xray_tracing_enabled = true
}

resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.credit_score.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.credit_api.execution_arn}/*/*"
}

output "score_api_url"         { value = "${aws_api_gateway_stage.main.invoke_url}/score" }
output "kinesis_stream_name"   { value = aws_kinesis_stream.transactions.name }
output "kinesis_stream_arn"    { value = aws_kinesis_stream.transactions.arn }
