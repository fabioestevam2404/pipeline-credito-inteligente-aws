#!/usr/bin/env bash
# ============================================================
# scripts/deploy_lambdas.sh
# Build (zip) e deploy das funções Lambda.
# Uso: bash scripts/deploy_lambdas.sh prod
# ============================================================

set -euo pipefail

ENV=${1:-"prod"}
DIST_DIR="dist"

echo "🚀 Deploy das Lambdas — Ambiente: ${ENV}"
mkdir -p "${DIST_DIR}"

# Função auxiliar para criar o zip de uma Lambda
build_lambda() {
    local LAMBDA_NAME=$1
    local SOURCE_FILE=$2
    local ZIP_FILE="${DIST_DIR}/${LAMBDA_NAME}.zip"

    echo "  📦 Building ${LAMBDA_NAME}..."

    # Cria dir temporário
    TMP_DIR=$(mktemp -d)
    cp "${SOURCE_FILE}" "${TMP_DIR}/handler.py"

    # Copia dependências compartilhadas
    mkdir -p "${TMP_DIR}/utils"
    cp src/utils/aws_helpers.py "${TMP_DIR}/utils/__init__.py" 2>/dev/null || true

    # Instala dependências da Lambda (apenas pacotes necessários)
    pip install boto3 requests tenacity \
        --target "${TMP_DIR}" \
        --quiet \
        --no-deps 2>/dev/null || true

    # Cria o zip
    (cd "${TMP_DIR}" && zip -r "${OLDPWD}/${ZIP_FILE}" . -q)
    rm -rf "${TMP_DIR}"

    echo "  ✅ ${ZIP_FILE} criado ($(du -sh "${ZIP_FILE}" | cut -f1))"
}

# Build das Lambdas
build_lambda "bureau_query"          "src/ingestion/lambda_bureau_query.py"
build_lambda "score_endpoint"        "src/scoring/lambda_credit_score.py"
build_lambda "bureau_batch"          "src/ingestion/lambda_bureau_query.py"  # Mesma base

echo ""
echo "📋 Zips criados em ${DIST_DIR}/:"
ls -lh "${DIST_DIR}/"

echo ""
echo "🔄 Atualizando funções no AWS Lambda..."

LAMBDAS=(
    "credit-bureau-query:${DIST_DIR}/bureau_query.zip"
    "credit-score-endpoint:${DIST_DIR}/score_endpoint.zip"
    "credit-bureau-batch-query:${DIST_DIR}/bureau_batch.zip"
)

for ENTRY in "${LAMBDAS[@]}"; do
    FUNC_NAME="${ENTRY%%:*}"
    ZIP_PATH="${ENTRY##*:}"

    echo "  🔄 Atualizando ${FUNC_NAME}..."
    aws lambda update-function-code \
        --function-name "${FUNC_NAME}" \
        --zip-file "fileb://${ZIP_PATH}" \
        --no-cli-pager \
        --output text \
        --query "FunctionArn" 2>/dev/null \
    && echo "  ✅ ${FUNC_NAME} atualizada" \
    || echo "  ⚠️  ${FUNC_NAME}: Lambda não encontrada (execute terraform apply primeiro)"
done

echo ""
echo "✅ Deploy das Lambdas concluído!"
