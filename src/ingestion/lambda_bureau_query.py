"""
src/ingestion/lambda_bureau_query.py
Lambda — Consulta individual de bureau (Serasa) para scoring em tempo real.
Persiste a resposta bruta no S3 Raw para auditoria e reprocessamento.

Invocado por: API Gateway (score request) ou Step Functions (batch)
"""
import hashlib
import json
import logging
import os
import time
from datetime import datetime

import boto3
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Clientes AWS (inicializados fora do handler para reutilização entre invocações)
s3      = boto3.client("s3")
secrets = boto3.client("secretsmanager")

# Configurações (via variáveis de ambiente)
RAW_BUCKET  = os.environ.get("RAW_BUCKET", "credit-pipeline-raw")
KMS_KEY_ARN = os.environ.get("KMS_KEY_ARN", "alias/credit-pipeline-key")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "prod")

# Cache do token de autenticação (válido por ~55 min) — "token" aqui é só a
# chave do dict, valor inicial None; a credencial real vem do Secrets Manager
# (get_bureau_credentials abaixo), não é um segredo hardcoded.
_token_cache: dict = {"token": None, "expires_at": 0}  # nosec B105


def get_bureau_credentials() -> dict:
    """Busca credenciais do bureau no Secrets Manager."""
    secret = secrets.get_secret_value(SecretId="credit/bureau/serasa")
    return json.loads(secret["SecretString"])


def get_auth_token() -> str:
    """
    Retorna token de autenticação do bureau, usando cache se ainda válido.
    Evita autenticação a cada requisição (melhora latência e evita rate limiting).
    """
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires_at"]:
        return _token_cache["token"]

    creds = get_bureau_credentials()
    response = requests.post(
        "https://api.serasaexperian.com.br/auth/token",
        json={
            "client_id":     creds["client_id"],
            "client_secret": creds["client_secret"],
            "grant_type":    "client_credentials",
        },
        timeout=10,
    )
    response.raise_for_status()
    data = response.json()

    _token_cache["token"]      = data["access_token"]
    _token_cache["expires_at"] = now + data.get("expires_in", 3600) - 300  # 5min margem
    logger.info("Token do bureau renovado com sucesso")
    return _token_cache["token"]


def query_bureau(cpf: str, token: str) -> dict:
    """
    Consulta score e dados de crédito na API do bureau.

    Args:
        cpf: CPF em claro (apenas dígitos) — NUNCA loga este valor
        token: Token de autenticação

    Returns:
        Dict com dados do bureau (score, restrições, etc.)
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
        "X-Request-ID":  f"credit-{int(time.time())}",
    }
    response = requests.get(
        f"https://api.serasaexperian.com.br/v1/pessoas/{cpf}/consolidado",
        headers=headers,
        timeout=15,  # SLA: bureau responde em até 10s; 15s é o timeout seguro
    )
    response.raise_for_status()
    return response.json()


def persist_to_s3(application_id: str, cpf_hash: str, bureau_data: dict,
                  queried_at: str) -> str:
    """
    Persiste resposta bruta do bureau no S3 para auditoria.
    Dados criptografados com KMS.

    Returns:
        S3 URI do arquivo salvo
    """
    today   = datetime.utcnow().strftime("%Y/%m/%d")
    s3_key  = f"bureau/serasa/{today}/{application_id}_{cpf_hash[:16]}.json"

    payload = {
        "application_id":  application_id,
        "cpf_hash":        cpf_hash,         # Nunca salvamos o CPF em claro
        "bureau_source":   "serasa",
        "bureau_response": bureau_data,
        "queried_at":      queried_at,
        "environment":     ENVIRONMENT,
    }

    s3.put_object(
        Bucket      = RAW_BUCKET,
        Key         = s3_key,
        Body        = json.dumps(payload, ensure_ascii=False),
        ContentType = "application/json",
        ServerSideEncryption = "aws:kms",
        SSEKMSKeyId = KMS_KEY_ARN,
    )
    return f"s3://{RAW_BUCKET}/{s3_key}"


def extract_score_fields(bureau_data: dict) -> dict:
    """
    Extrai campos padronizados da resposta do bureau.
    Isola a lógica de parsing do schema específico do Serasa.
    """
    return {
        "score_bureau":       bureau_data.get("scoreSerasa", {}).get("score", 0),
        "score_label":        bureau_data.get("scoreSerasa", {}).get("faixa", "DESCONHECIDO"),
        "qtd_restricoes":     len(bureau_data.get("restricoes", [])),
        "valor_total_dividas": sum(
            r.get("valor", 0) for r in bureau_data.get("restricoes", [])
        ),
        "possui_cheque_sem_fundo": bool(
            bureau_data.get("chequeSemFundo", {}).get("quantidade", 0) > 0
        ),
        "meses_negativado":   bureau_data.get("historicoNegativos", {}).get("meses", 0),
    }


def lambda_handler(event: dict, context) -> dict:
    """
    Handler principal da Lambda de consulta de bureau.

    Evento esperado:
        {
            "cpf": "12345678901",
            "application_id": "APP-2025-001",
            "request_id": "req-abc123"   (opcional)
        }

    Retorno:
        {
            "statusCode": 200,
            "application_id": "APP-2025-001",
            "cpf_hash": "abc123...",
            "score_bureau": 720,
            "qtd_restricoes": 0,
            "s3_path": "s3://...",
            "latency_ms": 145
        }
    """
    start_time = time.time()

    cpf            = event.get("cpf", "").strip()
    application_id = event.get("application_id", f"UNKNOWN-{int(time.time())}")

    # Validação de entrada
    if not cpf:
        return {"statusCode": 400, "error": "Campo 'cpf' obrigatório"}

    cpf_digits = "".join(filter(str.isdigit, cpf))
    if len(cpf_digits) != 11:
        return {"statusCode": 400, "error": "CPF deve ter 11 dígitos"}

    # Hash do CPF para logging seguro (LGPD)
    cpf_hash = hashlib.sha256(cpf_digits.encode()).hexdigest()
    logger.info(f"Consultando bureau | app={application_id} | cpf_hash={cpf_hash[:12]}***")

    queried_at = datetime.utcnow().isoformat() + "Z"

    try:
        # Consulta o bureau
        token       = get_auth_token()
        bureau_data = query_bureau(cpf_digits, token)

        # Persiste dados brutos para auditoria
        s3_path = persist_to_s3(application_id, cpf_hash, bureau_data, queried_at)

        # Extrai campos normalizados
        scores = extract_score_fields(bureau_data)

        latency_ms = int((time.time() - start_time) * 1000)
        logger.info(f"Bureau consultado com sucesso | score={scores['score_bureau']} | latência={latency_ms}ms")

        return {
            "statusCode":     200,
            "application_id": application_id,
            "cpf_hash":       cpf_hash,
            "queried_at":     queried_at,
            "s3_path":        s3_path,
            "latency_ms":     latency_ms,
            **scores,
        }

    except requests.exceptions.Timeout:
        logger.error(f"Timeout na consulta ao bureau | app={application_id}")
        return {"statusCode": 504, "error": "Bureau timeout — tente novamente em instantes"}

    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else 0
        logger.error(f"Erro HTTP bureau: {status} | app={application_id}")
        if status == 429:
            return {"statusCode": 429, "error": "Rate limit do bureau atingido"}
        if status == 404:
            return {"statusCode": 200, "score_bureau": 0, "qtd_restricoes": 0,
                    "not_found": True}  # CPF não encontrado no bureau
        return {"statusCode": 502, "error": f"Bureau retornou erro: {status}"}

    except Exception as e:
        logger.exception(f"Erro inesperado | app={application_id}: {e}")
        return {"statusCode": 500, "error": "Erro interno — verificar CloudWatch"}
