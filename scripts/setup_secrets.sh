#!/usr/bin/env bash
# ============================================================
# scripts/setup_secrets.sh
# Cria os segredos no AWS Secrets Manager antes do primeiro deploy.
# Uso: bash scripts/setup_secrets.sh --crm-host host --crm-user user --crm-pass pwd \
#                                    --bureau-client-id id --bureau-secret secret
# ============================================================

set -euo pipefail

# Parse de argumentos
while [[ $# -gt 0 ]]; do
    case $1 in
        --crm-host)         CRM_HOST="$2";           shift 2 ;;
        --crm-user)         CRM_USER="$2";           shift 2 ;;
        --crm-pass)         CRM_PASS="$2";           shift 2 ;;
        --bureau-client-id) BUREAU_CLIENT_ID="$2";   shift 2 ;;
        --bureau-secret)    BUREAU_SECRET="$2";      shift 2 ;;
        --region)           REGION="$2";             shift 2 ;;
        *) echo "Argumento desconhecido: $1"; exit 1 ;;
    esac
done

REGION="${REGION:-us-east-1}"

echo "🔐 Configurando segredos no AWS Secrets Manager — Região: ${REGION}"

# 1. Credenciais do CRM
echo "  📝 Criando segredo: credit/crm/database"
aws secretsmanager create-secret \
    --name "credit/crm/database" \
    --description "Credenciais do banco PostgreSQL do CRM" \
    --secret-string "{
        \"host\":     \"${CRM_HOST}\",
        \"port\":     \"5432\",
        \"database\": \"credit_db\",
        \"username\": \"${CRM_USER}\",
        \"password\": \"${CRM_PASS}\"
    }" \
    --region "${REGION}" \
    --no-cli-pager \
    2>/dev/null \
|| aws secretsmanager update-secret \
    --secret-id "credit/crm/database" \
    --secret-string "{
        \"host\":     \"${CRM_HOST}\",
        \"port\":     \"5432\",
        \"database\": \"credit_db\",
        \"username\": \"${CRM_USER}\",
        \"password\": \"${CRM_PASS}\"
    }" \
    --region "${REGION}" \
    --no-cli-pager

echo "  ✅ credit/crm/database configurado"

# 2. Credenciais do Bureau
echo "  📝 Criando segredo: credit/bureau/serasa"
aws secretsmanager create-secret \
    --name "credit/bureau/serasa" \
    --description "Credenciais API Serasa Experian" \
    --secret-string "{
        \"client_id\":     \"${BUREAU_CLIENT_ID}\",
        \"client_secret\": \"${BUREAU_SECRET}\",
        \"base_url\":      \"https://api.serasaexperian.com.br\"
    }" \
    --region "${REGION}" \
    --no-cli-pager \
    2>/dev/null \
|| aws secretsmanager update-secret \
    --secret-id "credit/bureau/serasa" \
    --secret-string "{
        \"client_id\":     \"${BUREAU_CLIENT_ID}\",
        \"client_secret\": \"${BUREAU_SECRET}\",
        \"base_url\":      \"https://api.serasaexperian.com.br\"
    }" \
    --region "${REGION}" \
    --no-cli-pager

echo "  ✅ credit/bureau/serasa configurado"

echo ""
echo "✅ Segredos configurados com sucesso!"
echo ""
echo "Para verificar:"
echo "  aws secretsmanager list-secrets --filter Key=name,Values=credit --region ${REGION}"
