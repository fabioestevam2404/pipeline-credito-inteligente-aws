"""
Lambda: Consulta de Bureau (Serasa/SPC)
Trigger: Step Functions ou API Gateway
"""
import hashlib
import json
import time
from datetime import datetime, timezone

import boto3
import requests

s3      = boto3.client("s3")
secrets = boto3.client("secretsmanager")

RAW_BUCKET  = __import__("os").environ["RAW_BUCKET"]
KMS_KEY_ID  = __import__("os").environ["KMS_KEY_ID"]
ENV         = __import__("os").environ["ENVIRONMENT"]
BUREAU_SECRET = __import__("os").environ["BUREAU_SECRET"]


def get_bureau_credentials():
    r = secrets.get_secret_value(SecretId=BUREAU_SECRET)
    return json.loads(r["SecretString"])


def get_bearer_token(creds: dict) -> str:
    """Autentica na API do bureau e retorna token."""
    r = requests.post(
        f"{creds['api_base_url']}/auth/token",
        json={"client_id": creds["client_id"], "client_secret": creds["client_secret"]},
        timeout=10,
        headers={"Content-Type": "application/json"}
    )
    r.raise_for_status()
    return r.json()["access_token"]


def query_bureau(cpf: str, token: str, api_base: str) -> dict:
    """Consulta score e restricoes para um CPF."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Score
    score_r = requests.get(
        f"{api_base}/v1/pessoas/{cpf}/score",
        headers=headers, timeout=15
    )
    score_r.raise_for_status()
    score_data = score_r.json()

    # Restricoes
    rest_r = requests.get(
        f"{api_base}/v1/pessoas/{cpf}/restricoes",
        headers=headers, timeout=15
    )
    rest_r.raise_for_status()
    rest_data = rest_r.json()

    return {
        "score_bureau"       : score_data.get("score", 0),
        "qtd_restricoes"     : rest_data.get("totalRestricoes", 0),
        "valor_total_dividas": rest_data.get("valorTotalDividas", 0.0),
        "maior_atraso_dias"  : rest_data.get("maiorAtraso", 0),
    }


def persist_to_s3(application_id: str, cpf_hash: str, payload: dict) -> str:
    """Salva resposta bruta no S3 para auditoria."""
    today  = datetime.now(timezone.utc)
    s3_key = (f"bureau/serasa/{today.year}/{today.month:02d}/{today.day:02d}/"
              f"{application_id}_{cpf_hash[:12]}.json")

    s3.put_object(
        Bucket      = RAW_BUCKET,
        Key         = s3_key,
        Body        = json.dumps(payload, ensure_ascii=False),
        ContentType = "application/json",
        ServerSideEncryption = "aws:kms",
        SSEKMSKeyId = KMS_KEY_ID
    )
    return f"s3://{RAW_BUCKET}/{s3_key}"


def lambda_handler(event, context):
    """
    Input:  { "application_id": "APP-001", "cpf": "12345678901" }
    Output: { "score_bureau": 750, "qtd_restricoes": 0, ... }
    """
    start = time.time()

    application_id = event["application_id"]
    cpf            = event["cpf"]
    cpf_hash       = hashlib.sha256(cpf.encode()).hexdigest()

    # NUNCA loga CPF em claro
    print(f"[Bureau] application_id={application_id} cpf_hash_prefix={cpf_hash[:8]}")

    try:
        creds  = get_bureau_credentials()
        token  = get_bearer_token(creds)
        result = query_bureau(cpf, token, creds["api_base_url"])

    except requests.exceptions.Timeout:
        print(f"[Bureau] TIMEOUT para {application_id}")
        # Retorna valores conservadores em caso de timeout
        result = {"score_bureau": 0, "qtd_restricoes": 1,
                  "valor_total_dividas": 0.0, "maior_atraso_dias": 0,
                  "bureau_status": "TIMEOUT"}

    except requests.exceptions.HTTPError as e:
        print(f"[Bureau] HTTP ERROR {e.response.status_code} para {application_id}")
        result = {"score_bureau": 0, "qtd_restricoes": 1,
                  "valor_total_dividas": 0.0, "maior_atraso_dias": 0,
                  "bureau_status": f"HTTP_{e.response.status_code}"}

    # Persiste no S3 independente do resultado
    payload = {
        "application_id": application_id,
        "cpf_hash"      : cpf_hash,
        "bureau_response": result,
        "queried_at"    : datetime.utcnow().isoformat(),
        "latency_ms"    : int((time.time() - start) * 1000)
    }
    s3_path = persist_to_s3(application_id, cpf_hash, payload)

    print(f"[Bureau] Concluido em {payload['latency_ms']}ms -> {s3_path}")

    return {
        "statusCode"    : 200,
        "application_id": application_id,
        "cpf_hash"      : cpf_hash,
        "s3_path"       : s3_path,
        **result
    }
