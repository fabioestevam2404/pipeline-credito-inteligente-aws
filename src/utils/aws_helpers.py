"""
src/utils/aws_helpers.py
Utilitários compartilhados para interação com serviços AWS.
Usados por todos os módulos do pipeline.
"""
import json
import hashlib
import logging
from datetime import datetime
from functools import lru_cache
from typing import Any, Optional

import boto3
from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

# ── Clientes AWS (singleton por sessão) ───────────────────────
@lru_cache(maxsize=None)
def get_client(service: str, region: str = "us-east-1"):
    """Retorna cliente AWS cacheado (evita reconexões desnecessárias)."""
    return boto3.client(service, region_name=region)


@lru_cache(maxsize=None)
def get_resource(service: str, region: str = "us-east-1"):
    """Retorna resource AWS cacheado."""
    return boto3.resource(service, region_name=region)


# ── Secrets Manager ──────────────────────────────────────────
@lru_cache(maxsize=32)
def get_secret(secret_name: str, region: str = "us-east-1") -> dict:
    """
    Busca segredo do AWS Secrets Manager.
    Cache em memória para evitar múltiplas chamadas na mesma execução.

    Args:
        secret_name: Nome ou ARN do segredo
        region: Região AWS

    Returns:
        Dict com os valores do segredo

    Raises:
        ClientError: Se o segredo não existir ou não houver permissão
    """
    client = get_client("secretsmanager", region)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret_str = response.get("SecretString", "{}")
        return json.loads(secret_str)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "ResourceNotFoundException":
            raise ValueError(f"Segredo não encontrado: {secret_name}") from e
        elif code == "AccessDeniedException":
            raise PermissionError(f"Sem permissão para acessar: {secret_name}") from e
        raise


# ── Segurança / LGPD ─────────────────────────────────────────
def hash_cpf(cpf: str) -> str:
    """
    Pseudoanonimiza CPF usando SHA-256.
    O hash é determinístico — o mesmo CPF sempre gera o mesmo hash,
    permitindo joins entre tabelas sem expor o CPF em claro.

    Args:
        cpf: CPF no formato "12345678901" (apenas dígitos)

    Returns:
        Hash SHA-256 hexadecimal (64 chars)
    """
    cpf_clean = "".join(filter(str.isdigit, cpf))
    if len(cpf_clean) != 11:
        raise ValueError(f"CPF inválido: deve ter 11 dígitos, recebeu {len(cpf_clean)}")
    return hashlib.sha256(cpf_clean.encode()).hexdigest()


def validate_cpf_format(cpf: str) -> bool:
    """Valida formato básico do CPF (não valida dígitos verificadores)."""
    digits = "".join(filter(str.isdigit, cpf))
    return len(digits) == 11 and not all(d == digits[0] for d in digits)


# ── S3 Helpers ────────────────────────────────────────────────
def s3_put_json(bucket: str, key: str, data: Any,
                kms_key_arn: Optional[str] = None) -> str:
    """
    Serializa data para JSON e salva no S3 com criptografia KMS.

    Returns:
        S3 URI completo (s3://bucket/key)
    """
    s3 = get_client("s3")
    kwargs = {
        "Bucket": bucket,
        "Key":    key,
        "Body":   json.dumps(data, ensure_ascii=False, default=str),
        "ContentType": "application/json",
    }
    if kms_key_arn:
        kwargs["ServerSideEncryption"] = "aws:kms"
        kwargs["SSEKMSKeyId"] = kms_key_arn

    s3.put_object(**kwargs)
    logger.debug(f"Salvo em s3://{bucket}/{key}")
    return f"s3://{bucket}/{key}"


def s3_key_exists(bucket: str, key: str) -> bool:
    """Verifica se um objeto existe no S3 sem baixar o conteúdo."""
    s3 = get_client("s3")
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


# ── CloudWatch Métricas ───────────────────────────────────────
def publish_metric(namespace: str, metric_name: str, value: float,
                   unit: str = "Count", dimensions: Optional[dict] = None):
    """Publica métrica customizada no CloudWatch."""
    cw = get_client("cloudwatch")
    metric = {
        "MetricName": metric_name,
        "Value":      value,
        "Unit":       unit,
        "Timestamp":  datetime.utcnow(),
    }
    if dimensions:
        metric["Dimensions"] = [
            {"Name": k, "Value": str(v)} for k, v in dimensions.items()
        ]
    cw.put_metric_data(Namespace=namespace, MetricData=[metric])


# ── Retry Decorator ───────────────────────────────────────────
def with_retry(max_attempts: int = 3, min_wait: int = 1, max_wait: int = 60):
    """
    Decorator de retry com backoff exponencial para chamadas de API externas.

    Uso:
        @with_retry(max_attempts=3)
        def chamar_bureau(cpf):
            ...
    """
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
        reraise=True,
    )


# ── Particionamento S3 ────────────────────────────────────────
def s3_partition_path(base: str, date: Optional[datetime] = None) -> str:
    """
    Gera path S3 particionado por data (Hive-style).

    Exemplo:
        s3_partition_path("s3://bucket/prefix", datetime(2025,1,20))
        → "s3://bucket/prefix/year=2025/month=01/day=20"
    """
    d = date or datetime.utcnow()
    return f"{base.rstrip('/')}/year={d.year}/month={d.month:02d}/day={d.day:02d}"
