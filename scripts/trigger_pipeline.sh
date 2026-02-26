#!/usr/bin/env bash
# ============================================================
# scripts/trigger_pipeline.sh
# Dispara execução manual do Step Functions pipeline.
# Uso: bash scripts/trigger_pipeline.sh [YYYY-MM-DD] [ENV]
# Exemplo: bash scripts/trigger_pipeline.sh 2025-01-20 prod
# ============================================================

set -euo pipefail

RUN_DATE=${1:-$(date -u +%Y-%m-%d)}
ENV=${2:-"prod"}
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=$(aws configure get region || echo "us-east-1")
SFN_NAME="credit-pipeline-${ENV}"

SFN_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${SFN_NAME}"

echo "🚀 Iniciando pipeline de crédito"
echo "   Data de processamento: ${RUN_DATE}"
echo "   Ambiente: ${ENV}"
echo "   State Machine: ${SFN_NAME}"
echo ""

INPUT=$(cat <<EOF
{
    "run_date": "${RUN_DATE}",
    "triggered_by": "manual_script",
    "environment": "${ENV}",
    "operator": "$(aws sts get-caller-identity --query Arn --output text)"
}
EOF
)

EXECUTION_ARN=$(aws stepfunctions start-execution \
    --state-machine-arn "${SFN_ARN}" \
    --name "manual-run-${RUN_DATE}-$(date +%H%M%S)" \
    --input "${INPUT}" \
    --query "executionArn" \
    --output text)

echo "✅ Execução iniciada!"
echo "   ARN: ${EXECUTION_ARN}"
echo ""
echo "🔍 Para monitorar:"
echo "   Console: https://${REGION}.console.aws.amazon.com/states/home#/executions/details/${EXECUTION_ARN}"
echo ""
echo "   CLI (polling a cada 30s):"
echo "   watch -n 30 'aws stepfunctions describe-execution \\"
echo "     --execution-arn ${EXECUTION_ARN} \\"
echo "     --query \"{status:status,started:startDate,stopped:stopDate}\"'"
