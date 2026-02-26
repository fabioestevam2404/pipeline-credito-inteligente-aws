# ─── IAM Module ───────────────────────────────────────────────────────────────
# Roles e policies com least-privilege por serviço

variable "environment"          {}
variable "account_id"           {}
variable "aws_region"           {}
variable "credit_key_arn"       {}
variable "raw_bucket_arn"       {}
variable "silver_bucket_arn"    {}
variable "gold_bucket_arn"      {}
variable "compliance_bucket_arn" {}
variable "artifacts_bucket_arn" {}

# ── Glue Role ─────────────────────────────────────────────────────────────────
resource "aws_iam_role" "glue_role" {
  name = "credit-glue-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })

  tags = { Environment = var.environment }
}

resource "aws_iam_role_policy" "glue_s3_policy" {
  name = "glue-s3-access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadRaw"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [
          var.raw_bucket_arn,
          "${var.raw_bucket_arn}/*"
        ]
      },
      {
        Sid    = "WriteSilver"
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          var.silver_bucket_arn,
          "${var.silver_bucket_arn}/*"
        ]
      },
      {
        Sid    = "WriteGold"
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
        Resource = [
          var.gold_bucket_arn,
          "${var.gold_bucket_arn}/*"
        ]
      },
      {
        Sid    = "ReadArtifacts"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          var.artifacts_bucket_arn,
          "${var.artifacts_bucket_arn}/*"
        ]
      },
      {
        Sid    = "KMSAccess"
        Effect = "Allow"
        Action = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
        Resource = [var.credit_key_arn]
      },
      {
        Sid    = "GlueCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:CreateDatabase",
          "glue:GetTable", "glue:CreateTable", "glue:UpdateTable",
          "glue:GetPartitions", "glue:BatchCreatePartition",
          "glue:GetDataQualityResult", "glue:StartDataQualityRuleRecommendationRun"
        ]
        Resource = "*"
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup", "logs:CreateLogStream",
          "logs:PutLogEvents", "logs:GetLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${var.account_id}:log-group:/aws-glue/*"
      },
      {
        Sid    = "SecretsManager"
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue"]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:${var.account_id}:secret:credit-pipeline/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_managed" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# ── Lambda Role ───────────────────────────────────────────────────────────────
resource "aws_iam_role" "lambda_role" {
  name = "credit-lambda-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  name = "lambda-credit-access"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3RawWrite"
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:GetObject"]
        Resource = "${var.raw_bucket_arn}/*"
      },
      {
        Sid    = "S3ComplianceWrite"
        Effect = "Allow"
        Action = ["s3:PutObject"]
        Resource = "${var.compliance_bucket_arn}/*"
      },
      {
        Sid    = "KMSAccess"
        Effect = "Allow"
        Action = ["kms:Decrypt", "kms:GenerateDataKey"]
        Resource = [var.credit_key_arn]
      },
      {
        Sid    = "SecretsAccess"
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue"]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:${var.account_id}:secret:credit-pipeline/*"
      },
      {
        Sid    = "DynamoDBAudit"
        Effect = "Allow"
        Action = ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:UpdateItem",
                  "dynamodb:Scan", "dynamodb:DeleteItem"]
        Resource = "arn:aws:dynamodb:${var.aws_region}:${var.account_id}:table/credit-score-audit"
      },
      {
        Sid    = "FeatureStoreRuntime"
        Effect = "Allow"
        Action = ["sagemaker:GetRecord", "sagemaker:PutRecord", "sagemaker:DeleteRecord"]
        Resource = "*"
      },
      {
        Sid    = "SageMakerInvoke"
        Effect = "Allow"
        Action = ["sagemaker:InvokeEndpoint"]
        Resource = "arn:aws:sagemaker:${var.aws_region}:${var.account_id}:endpoint/credit-score-*"
      },
      {
        Sid    = "CloudWatchMetrics"
        Effect = "Allow"
        Action = ["cloudwatch:PutMetricData"]
        Resource = "*"
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:${var.aws_region}:${var.account_id}:*"
      },
      {
        Sid    = "KinesisRead"
        Effect = "Allow"
        Action = ["kinesis:GetRecords", "kinesis:GetShardIterator",
                  "kinesis:DescribeStream", "kinesis:ListStreams"]
        Resource = "arn:aws:kinesis:${var.aws_region}:${var.account_id}:stream/credit-transactions"
      }
    ]
  })
}

# ── Step Functions Role ───────────────────────────────────────────────────────
resource "aws_iam_role" "stepfunctions_role" {
  name = "credit-stepfunctions-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "stepfunctions_policy" {
  name = "stepfunctions-credit-access"
  role = aws_iam_role.stepfunctions_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueJobRun"
        Effect = "Allow"
        Action = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
        Resource = "*"
      },
      {
        Sid    = "LambdaInvoke"
        Effect = "Allow"
        Action = ["lambda:InvokeFunction"]
        Resource = "arn:aws:lambda:${var.aws_region}:${var.account_id}:function:credit-*"
      },
      {
        Sid    = "SNSPublish"
        Effect = "Allow"
        Action = ["sns:Publish"]
        Resource = "arn:aws:sns:${var.aws_region}:${var.account_id}:credit-pipeline-alerts"
      },
      {
        Sid    = "SageMakerProcessing"
        Effect = "Allow"
        Action = ["sagemaker:CreateProcessingJob", "sagemaker:DescribeProcessingJob",
                  "sagemaker:StopProcessingJob"]
        Resource = "*"
      },
      {
        Sid    = "CloudWatchEvents"
        Effect = "Allow"
        Action = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
        Resource = "*"
      },
      {
        Sid    = "XRayTracing"
        Effect = "Allow"
        Action = ["xray:PutTraceSegments", "xray:PutTelemetryRecords"]
        Resource = "*"
      }
    ]
  })
}

# ── SageMaker Role ────────────────────────────────────────────────────────────
resource "aws_iam_role" "sagemaker_role" {
  name = "credit-sagemaker-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "sagemaker.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "sagemaker_full" {
  role       = aws_iam_role.sagemaker_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"
}

resource "aws_iam_role_policy" "sagemaker_s3" {
  name = "sagemaker-s3-access"
  role = aws_iam_role.sagemaker_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      Resource = [
        var.gold_bucket_arn, "${var.gold_bucket_arn}/*",
        var.artifacts_bucket_arn, "${var.artifacts_bucket_arn}/*"
      ]
    }]
  })
}

output "glue_role_arn"          { value = aws_iam_role.glue_role.arn }
output "lambda_role_arn"        { value = aws_iam_role.lambda_role.arn }
output "stepfunctions_role_arn" { value = aws_iam_role.stepfunctions_role.arn }
output "sagemaker_role_arn"     { value = aws_iam_role.sagemaker_role.arn }
