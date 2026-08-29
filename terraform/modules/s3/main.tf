# ─── S3 Module ────────────────────────────────────────────────────────────────
# Buckets para as 3 camadas + compliance + auditoria

variable "environment"      {}
variable "account_id"       {}
variable "credit_key_arn"   {}
variable "audit_key_arn"    {}

locals {
  buckets = {
    raw        = "credit-pipeline-raw-${var.account_id}-${var.environment}"
    silver     = "credit-pipeline-silver-${var.account_id}-${var.environment}"
    gold       = "credit-pipeline-gold-${var.account_id}-${var.environment}"
    compliance = "credit-pipeline-compliance-${var.account_id}-${var.environment}"
    artifacts  = "credit-pipeline-artifacts-${var.account_id}-${var.environment}"
  }
}

# ── Raw Bucket ────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "raw" {
  bucket = local.buckets.raw
  tags   = { Layer = "Bronze", Environment = var.environment }
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.credit_key_arn
    }
    bucket_key_enabled = true  # Reduz chamadas KMS em até 99% (custo)
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "raw-retention-policy"
    status = "Enabled"

    # Move para Infrequent Access após 30 dias
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    # Move para Glacier após 60 dias
    transition {
      days          = 60
      storage_class = "GLACIER"
    }

    # Deleta após 90 dias (LGPD: minimização de dados)
    expiration {
      days = 90
    }

    # Limpa versões antigas após 7 dias
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

# ── Silver Bucket ─────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "silver" {
  bucket = local.buckets.silver
  tags   = { Layer = "Silver", Environment = var.environment }
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.credit_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id

  rule {
    id     = "silver-retention"
    status = "Enabled"

    transition {
      days          = 180
      storage_class = "STANDARD_IA"
    }

    # Silver retido por 2 anos
    expiration {
      days = 730
    }
  }
}

# ── Gold Bucket ───────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "gold" {
  bucket = local.buckets.gold
  tags   = { Layer = "Gold", Environment = var.environment }
}

resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.credit_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id

  rule {
    id     = "gold-retention"
    status = "Enabled"

    transition {
      days          = 365
      storage_class = "STANDARD_IA"
    }

    # Gold retido por 5 anos (features para retraining)
    expiration {
      days = 1825
    }
  }
}

# ── Compliance Bucket (Imutável — LGPD/BCB) ──────────────────────────────────
resource "aws_s3_bucket" "compliance" {
  bucket = local.buckets.compliance
  tags   = { Layer = "Compliance", Environment = var.environment }
}

resource "aws_s3_bucket_versioning" "compliance" {
  bucket = aws_s3_bucket.compliance.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "compliance" {
  bucket = aws_s3_bucket.compliance.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.audit_key_arn
    }
  }
}

# Object Lock: impede deleção de logs de auditoria (WORM)
resource "aws_s3_bucket_object_lock_configuration" "compliance" {
  bucket = aws_s3_bucket.compliance.id

  rule {
    default_retention {
      mode  = "COMPLIANCE"  # Nem admin consegue deletar antes do prazo
      years = 5             # 5 anos exigidos pelo Banco Central
    }
  }
}

resource "aws_s3_bucket_public_access_block" "compliance" {
  bucket                  = aws_s3_bucket.compliance.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Artifacts Bucket (Glue scripts, modelos) ──────────────────────────────────
resource "aws_s3_bucket" "artifacts" {
  bucket = local.buckets.artifacts
  tags   = { Layer = "Artifacts", Environment = var.environment }
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.credit_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "raw_bucket_name"        { value = aws_s3_bucket.raw.id }
output "silver_bucket_name"     { value = aws_s3_bucket.silver.id }
output "gold_bucket_name"       { value = aws_s3_bucket.gold.id }
output "compliance_bucket_name" { value = aws_s3_bucket.compliance.id }
output "artifacts_bucket_name"  { value = aws_s3_bucket.artifacts.id }
output "raw_bucket_arn"         { value = aws_s3_bucket.raw.arn }
output "silver_bucket_arn"      { value = aws_s3_bucket.silver.arn }
output "gold_bucket_arn"        { value = aws_s3_bucket.gold.arn }
