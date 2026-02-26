#!/bin/bash
# ─── Bootstrap: Configura o ambiente pela primeira vez ────────────────────────
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

info()    { echo -e "${GREEN}[INFO]${NC} $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

info "=== Bootstrap Pipeline de Credito AWS ==="

# Verifica dependencias
command -v aws       >/dev/null 2>&1 || error "AWS CLI nao encontrado. Instale: https://aws.amazon.com/cli/"
command -v terraform >/dev/null 2>&1 || error "Terraform nao encontrado. Instale: https://terraform.io"
command -v python3   >/dev/null 2>&1 || error "Python 3 nao encontrado"
command -v zip       >/dev/null 2>&1 || error "zip nao encontrado (apt-get install zip)"

# Verifica configuracao AWS
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null) || \
    error "AWS CLI nao configurado. Execute: aws configure"
REGION=$(aws configure get region || echo "us-east-1")

info "Account ID : $ACCOUNT_ID"
info "Region     : $REGION"

# Cria bucket de state do Terraform
STATE_BUCKET="terraform-state-credit-pipeline-${ACCOUNT_ID}"
if ! aws s3 ls "s3://$STATE_BUCKET" >/dev/null 2>&1; then
    info "Criando bucket de state Terraform: $STATE_BUCKET"
    aws s3 mb "s3://$STATE_BUCKET" --region "$REGION"
    aws s3api put-bucket-versioning \
        --bucket "$STATE_BUCKET" \
        --versioning-configuration Status=Enabled
    aws s3api put-bucket-encryption \
        --bucket "$STATE_BUCKET" \
        --server-side-encryption-configuration \
        '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"aws:kms"}}]}'
else
    info "Bucket de state ja existe: $STATE_BUCKET"
fi

# Cria tabela DynamoDB para lock do Terraform
LOCK_TABLE="terraform-state-lock"
if ! aws dynamodb describe-table --table-name "$LOCK_TABLE" >/dev/null 2>&1; then
    info "Criando tabela de lock Terraform: $LOCK_TABLE"
    aws dynamodb create-table \
        --table-name "$LOCK_TABLE" \
        --attribute-definitions AttributeName=LockID,AttributeType=S \
        --key-schema AttributeName=LockID,KeyType=HASH \
        --billing-mode PAY_PER_REQUEST \
        --region "$REGION"
else
    info "Tabela de lock ja existe: $LOCK_TABLE"
fi

# Cria secrets placeholder no Secrets Manager
create_secret_if_not_exists() {
    local name=$1
    local value=$2
    if ! aws secretsmanager describe-secret --secret-id "$name" >/dev/null 2>&1; then
        aws secretsmanager create-secret \
            --name "$name" \
            --description "Credit Pipeline Secret" \
            --secret-string "$value" \
            --region "$REGION"
        info "Secret criado: $name"
    else
        warn "Secret ja existe: $name (nao sobrescrito)"
    fi
}

create_secret_if_not_exists \
    "credit-pipeline/crm-db" \
    '{"username":"glue_reader","password":"SUBSTITUA_A_SENHA_REAL"}'

create_secret_if_not_exists \
    "credit-pipeline/bureau/serasa" \
    '{"client_id":"SUBSTITUA","client_secret":"SUBSTITUA","api_base_url":"https://api.serasaexperian.com.br"}'

# Copia arquivo de variaveis de exemplo
if [ ! -f "terraform/envs/prod/prod.tfvars" ]; then
    cp terraform/envs/prod/prod.tfvars.example terraform/envs/prod/prod.tfvars
    warn "IMPORTANTE: Edite terraform/envs/prod/prod.tfvars com seus valores reais!"
fi

info ""
info "=== Bootstrap concluido! ==="
info ""
info "Proximos passos:"
info "  1. Edite terraform/envs/prod/prod.tfvars"
info "  2. Atualize os secrets no Secrets Manager:"
info "     aws secretsmanager put-secret-value --secret-id credit-pipeline/crm-db --secret-string '{...}'"
info "  3. Execute: ./scripts/deploy.sh --component infra"
