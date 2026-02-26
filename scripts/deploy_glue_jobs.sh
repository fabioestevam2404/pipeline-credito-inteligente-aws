#!/usr/bin/env bash
# ============================================================
# scripts/deploy_glue_jobs.sh
# Deploy (upload) dos scripts Glue para o S3 e atualização dos jobs.
# Uso: bash scripts/deploy_glue_jobs.sh prod
# ============================================================

set -euo pipefail

ENV=${1:-"prod"}
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
SCRIPTS_BUCKET="credit-pipeline-raw-${ACCOUNT_ID}-${ENV}"
SCRIPTS_PREFIX="glue-scripts"

echo "🚀 Deploy dos Glue Jobs — Ambiente: ${ENV}"
echo "   Bucket: s3://${SCRIPTS_BUCKET}/${SCRIPTS_PREFIX}/"

# Upload de cada script
declare -A JOBS=(
    ["ingest_crm"]="src/ingestion/glue_ingest_crm.py"
    ["silver_processing"]="src/processing/glue_silver_processing.py"
    ["feature_engineering"]="src/features/glue_feature_engineering.py"
)

for JOB_NAME in "${!JOBS[@]}"; do
    SCRIPT_PATH="${JOBS[$JOB_NAME]}"
    S3_KEY="${SCRIPTS_PREFIX}/${JOB_NAME}.py"

    echo "  📤 Uploading ${SCRIPT_PATH} → s3://${SCRIPTS_BUCKET}/${S3_KEY}"
    aws s3 cp "${SCRIPT_PATH}" "s3://${SCRIPTS_BUCKET}/${S3_KEY}" \
        --sse aws:kms \
        --sse-kms-key-id "alias/credit-pipeline-key-${ENV}"

    echo "  ✅ ${JOB_NAME} atualizado"
done

echo ""
echo "✅ Todos os Glue Jobs atualizados com sucesso!"
echo ""
echo "Para testar manualmente:"
echo "  aws glue start-job-run --job-name credit-ingest-crm \\"
echo "    --arguments '{\"--run_date\":\"$(date +%Y-%m-%d)\"}'"
