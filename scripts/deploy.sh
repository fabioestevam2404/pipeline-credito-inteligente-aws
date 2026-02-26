#!/bin/bash
# ─── Deploy: Implanta componentes do pipeline ──────────────────────────────────
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

COMPONENT="${1:-}"
ENV="${2:-prod}"

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=$(aws configure get region || echo "us-east-1")
ARTIFACTS_BUCKET="credit-pipeline-artifacts-${ACCOUNT_ID}-${ENV}"

deploy_infra() {
    info "Implantando infraestrutura Terraform..."
    cd terraform/envs/$ENV
    terraform init -upgrade
    terraform plan -var-file=prod.tfvars -out=tfplan
    terraform apply tfplan
    cd -
    info "Infraestrutura implantada com sucesso!"
}

deploy_glue() {
    info "Fazendo upload dos Glue Jobs..."
    for job in glue_jobs/*.py; do
        filename=$(basename "$job")
        aws s3 cp "$job" "s3://$ARTIFACTS_BUCKET/glue-scripts/$filename"
        info "  Uploaded: $filename"
    done
    info "Glue Jobs atualizados!"
}

deploy_lambda() {
    info "Implantando Lambda functions..."

    for func_dir in lambda_functions/*/; do
        func_name=$(basename "$func_dir")
        info "  Empacotando $func_name..."

        cd "$func_dir"

        # Instala dependencias se requirements.txt existir
        if [ -f requirements.txt ]; then
            pip install -r requirements.txt -t ./package --quiet
            cp *.py ./package/
            cd package && zip -r "../${func_name}.zip" . -q && cd ..
            rm -rf package
        else
            zip -j "${func_name}.zip" *.py -q
        fi

        # Atualiza o codigo da Lambda
        lambda_full_name="credit-${func_name//_/-}-${ENV}"
        aws lambda update-function-code \
            --function-name "$lambda_full_name" \
            --zip-file "fileb://${func_name}.zip" \
            --region "$REGION" \
            --no-cli-pager >/dev/null

        rm -f "${func_name}.zip"
        cd - >/dev/null
        info "  $func_name atualizado!"
    done
}

deploy_sagemaker() {
    info "Fazendo upload dos scripts SageMaker..."
    aws s3 cp sagemaker/feature_store/ingest_features.py \
        "s3://$ARTIFACTS_BUCKET/sagemaker-scripts/ingest_features.py"
    info "Scripts SageMaker atualizados!"
}

validate() {
    info "=== Validacao do Pipeline ==="

    # Verifica Glue Jobs
    for job in ingest_crm silver_processing feature_engineering; do
        STATUS=$(aws glue get-job --job-name "credit-${job//_/-}-${ENV}" \
            --query 'Job.Name' --output text 2>/dev/null || echo "NAO_ENCONTRADO")
        if [ "$STATUS" != "NAO_ENCONTRADO" ]; then
            info "  Glue Job credit-${job//_/-}-${ENV}: OK"
        else
            warn "  Glue Job credit-${job//_/-}-${ENV}: NAO ENCONTRADO"
        fi
    done

    # Verifica State Machine
    SFN=$(aws stepfunctions list-state-machines \
        --query "stateMachines[?contains(name,'credit-pipeline-${ENV}')].name" \
        --output text 2>/dev/null || echo "")
    [ -n "$SFN" ] && info "  Step Functions $SFN: OK" || warn "  Step Functions nao encontrado"

    # Verifica Lambda de score
    LAMBDA=$(aws lambda get-function --function-name "credit-score-${ENV}" \
        --query 'Configuration.FunctionName' --output text 2>/dev/null || echo "")
    [ -n "$LAMBDA" ] && info "  Lambda $LAMBDA: OK" || warn "  Lambda credit-score-${ENV} nao encontrado"

    info "Validacao concluida!"
}

# ── Dispatcher ─────────────────────────────────────────────────────────────
case "$COMPONENT" in
    --component)
        case "$2" in
            infra)       deploy_infra ;;
            glue)        deploy_glue ;;
            lambda)      deploy_lambda ;;
            sagemaker)   deploy_sagemaker ;;
            all)
                deploy_infra
                deploy_glue
                deploy_lambda
                deploy_sagemaker
                ;;
            *) error "Componente invalido: $2. Use: infra|glue|lambda|sagemaker|all" ;;
        esac
        ;;
    --validate) validate ;;
    *)
        echo "Uso: $0 --component [infra|glue|lambda|sagemaker|all] [env]"
        echo "     $0 --validate [env]"
        exit 1
        ;;
esac
