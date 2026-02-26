# ─── KMS Module ───────────────────────────────────────────────────────────────
# Chaves de criptografia para dados sensíveis (CPF, renda, score)

variable "environment"  {}
variable "account_id"   {}
variable "aws_region"   {}

resource "aws_kms_key" "credit_key" {
  description             = "KMS key para dados sensíveis de crédito (LGPD) — ${var.environment}"
  deletion_window_in_days = 30
  enable_key_rotation     = true  # Rotação automática anual obrigatória

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "Enable IAM Root Policies"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${var.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid    = "Allow Glue Service"
        Effect = "Allow"
        Principal = { Service = "glue.amazonaws.com" }
        Action   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
        Resource = "*"
      },
      {
        Sid    = "Allow Lambda Service"
        Effect = "Allow"
        Principal = { Service = "lambda.amazonaws.com" }
        Action   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
        Resource = "*"
      },
      {
        Sid    = "Allow SageMaker Service"
        Effect = "Allow"
        Principal = { Service = "sagemaker.amazonaws.com" }
        Action   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey",
                    "kms:CreateGrant", "kms:ListGrants"]
        Resource = "*"
      }
    ]
  })

  tags = {
    Name        = "credit-pipeline-kms-${var.environment}"
    Environment = var.environment
    Team        = "data-engineering"
    Compliance  = "LGPD"
  }
}

resource "aws_kms_alias" "credit_key_alias" {
  name          = "alias/credit-pipeline-${var.environment}"
  target_key_id = aws_kms_key.credit_key.key_id
}

# Chave separada para logs de auditoria (imutável por regulação)
resource "aws_kms_key" "audit_key" {
  description             = "KMS key para logs de auditoria regulatória — ${var.environment}"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  tags = {
    Name        = "credit-audit-kms-${var.environment}"
    Environment = var.environment
    Compliance  = "BCB-LGPD"
  }
}

resource "aws_kms_alias" "audit_key_alias" {
  name          = "alias/credit-audit-${var.environment}"
  target_key_id = aws_kms_key.audit_key.key_id
}

output "credit_key_arn"   { value = aws_kms_key.credit_key.arn }
output "credit_key_id"    { value = aws_kms_key.credit_key.key_id }
output "audit_key_arn"    { value = aws_kms_key.audit_key.arn }
