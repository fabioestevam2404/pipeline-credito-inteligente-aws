"""
src/scoring/lambda_credit_score.py
Lambda — Endpoint de score de crédito em tempo real.

Fluxo de uma requisição (<200ms target):
  1. Valida entrada (CPF hash + application_id)
  2. Busca features no SageMaker Feature Store Online (<10ms)
  3. Invoca endpoint SageMaker (<50ms)
  4. Aplica política de crédito (score → decisão)
  5. Salva auditoria no DynamoDB
  6. Retorna resultado

Invocado por: API Gateway (REST)
"""
import json
import logging
import os
import time
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ENDPOINT_NAME = os.environ.get("ENDPOINT_NAME", "credit-score-model-v1")
FEATURE_GROUP = os.environ.get("FEATURE_GROUP", "credit-features-v1")
AUDIT_TABLE   = os.environ.get("AUDIT_TABLE",   "credit-score-audit")
ENVIRONMENT   = os.environ.get("ENVIRONMENT",   "prod")

# Clientes AWS (inicializados fora do handler para reutilização entre invocações warm)
sm_runtime  = boto3.client("sagemaker-runtime")
fs_runtime  = boto3.client("sagemaker-featurestore-runtime")
dynamodb    = boto3.resource("dynamodb")
audit_table = dynamodb.Table(AUDIT_TABLE)
cw          = boto3.client("cloudwatch")

# Ordem das features (deve ser IDÊNTICA ao treinamento do modelo)
FEATURE_ORDER = [
    "faixa_renda_log", "renda_padronizada",
    "dias_relacionamento", "anos_relacionamento", "cliente_antigo",
    "limite_aprovado", "qtd_contratos_ativos",
    "score_externo_norm", "score_risco_bureau",
    "num_restricoes", "divida_total", "flag_cheque",
    "meses_negativado", "flag_inadimplente",
    "qtd_tx_30d", "volume_tx_30d", "ticket_medio_30d", "ratio_credito_30d",
    "qtd_tx_90d", "volume_tx_90d", "ticket_medio_90d", "ratio_credito_90d",
    "qtd_tx_180d", "volume_tx_180d", "ratio_credito_180d",
    "utilizacao_limite", "tendencia_volume",
]

# Política de crédito (configurável por produto/segmento)
CREDIT_POLICY = {
    "APROVADO":       700,   # score >= 700 → aprovado automaticamente
    "ANALISE_MANUAL": 500,   # 500 <= score < 700 → análise humana
    # score < 500 → negado automaticamente
}


def get_features_online(cpf_hash: str) -> dict | None:
    """
    Busca features no Feature Store Online.
    Latência esperada: 5-15ms.

    Returns:
        Dict {feature_name: value} ou None se CPF não encontrado
    """
    try:
        response = fs_runtime.get_record(
            FeatureGroupName=FEATURE_GROUP,
            RecordIdentifierValueAsString=cpf_hash,
        )
        if not response.get("Record"):
            return None

        return {
            feat["FeatureName"]: feat["ValueAsString"]
            for feat in response["Record"]
        }
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return None
        raise


def build_feature_vector(features: dict) -> list:
    """
    Constrói o vetor de features na ordem correta para o modelo.
    Features ausentes recebem valor 0.0 (defaults conservadores).

    Returns:
        Lista de floats na ordem definida em FEATURE_ORDER
    """
    vector = []
    missing = []
    for feat_name in FEATURE_ORDER:
        val = features.get(feat_name)
        if val is None:
            missing.append(feat_name)
            vector.append(0.0)
        else:
            try:
                vector.append(float(val))
            except (ValueError, TypeError):
                missing.append(feat_name)
                vector.append(0.0)

    if missing:
        logger.warning(f"Features ausentes (usando 0.0): {missing}")

    return vector


def invoke_model(feature_vector: list) -> dict:
    """
    Invoca o endpoint SageMaker e retorna predição.
    Latência esperada: 20-80ms (depende do tipo de instância do endpoint).

    Returns:
        {
            "probability": 0.72,    # Probabilidade de bom pagador
            "score": 720            # Score normalizado 0-1000
        }
    """
    payload = json.dumps({"instances": [feature_vector]})
    response = sm_runtime.invoke_endpoint(
        EndpointName=ENDPOINT_NAME,
        ContentType="application/json",
        Accept="application/json",
        Body=payload,
    )
    result = json.loads(response["Body"].read().decode("utf-8"))

    # Extrai probabilidade (formato depende do framework do modelo)
    # XGBoost: predictions[0]
    # SKLearn: predictions[0][1] para classe 1 (bom pagador)
    raw_prob = result.get("predictions", [{}])[0]
    if isinstance(raw_prob, dict):
        probability = raw_prob.get("probability", raw_prob.get("score", 0.0))
    else:
        probability = float(raw_prob)

    # Normaliza probabilidade para score 0-1000
    score = min(1000, max(0, int(probability * 1000)))

    return {"probability": probability, "score": score}


def apply_credit_policy(score: int) -> dict:
    """
    Aplica política de crédito e retorna decisão com justificativa.

    Returns:
        {
            "decision": "APROVADO" | "ANALISE_MANUAL" | "NEGADO",
            "reason": "...",
            "score_band": "ALTO" | "MEDIO" | "BAIXO"
        }
    """
    if score >= CREDIT_POLICY["APROVADO"]:
        return {
            "decision":   "APROVADO",
            "reason":     f"Score {score} acima do threshold de aprovação automática (700)",
            "score_band": "ALTO",
        }
    elif score >= CREDIT_POLICY["ANALISE_MANUAL"]:
        return {
            "decision":   "ANALISE_MANUAL",
            "reason":     f"Score {score} requer análise humana (500-699)",
            "score_band": "MEDIO",
        }
    else:
        return {
            "decision":   "NEGADO",
            "reason":     f"Score {score} abaixo do threshold mínimo (500)",
            "score_band": "BAIXO",
        }


def save_audit_record(application_id: str, cpf_hash: str,
                      score: int, decision: dict,
                      features: dict, latency_ms: int):
    """
    Salva snapshot completo da decisão no DynamoDB para auditoria regulatória.

    Requisito BCB: toda decisão de crédito deve ser auditável por 5 anos,
    incluindo quais dados foram usados no momento da decisão.
    TTL automático configurado na tabela.
    """
    now = datetime.utcnow()
    audit_table.put_item(Item={
        "application_id": application_id,
        "decision_ts":    now.isoformat() + "Z",
        "cpf_hash":       cpf_hash,
        "score":          score,
        "decision":       decision["decision"],
        "score_band":     decision["score_band"],
        "reason":         decision["reason"],
        # Snapshot das features — crucial para explicabilidade e auditoria
        "features_snapshot": {k: str(v) for k, v in
                              zip(FEATURE_ORDER, features) if v != 0.0},
        "model_endpoint":    ENDPOINT_NAME,
        "model_version":     "v1.0",
        "environment":       ENVIRONMENT,
        "latency_ms":        latency_ms,
        # TTL: 5 anos em Unix timestamp
        "ttl_expiry":        int(now.timestamp()) + (5 * 365 * 24 * 3600),
    })


def publish_score_metrics(score: int, decision: str, latency_ms: int):
    """Publica métricas de scoring no CloudWatch para o dashboard."""
    cw.put_metric_data(
        Namespace  = "CreditPipeline/Scoring",
        MetricData = [
            {"MetricName": "ScoreRequests",  "Value": 1.0,          "Unit": "Count"},
            {"MetricName": "ScoreLatencyMs", "Value": float(latency_ms), "Unit": "Milliseconds"},
            {"MetricName": "ScoreValue",     "Value": float(score),  "Unit": "None"},
            {
                "MetricName": f"Decision_{decision}",
                "Value": 1.0, "Unit": "Count",
                "Dimensions": [{"Name": "Decision", "Value": decision}]
            },
        ]
    )


def lambda_handler(event: dict, context) -> dict:
    """
    Handler do endpoint de score de crédito.

    Corpo da requisição (via API Gateway):
        {
            "application_id": "APP-2025-00123",
            "cpf_hash": "a3f4b5c6...64chars"
        }

    Resposta:
        {
            "statusCode": 200,
            "body": {
                "application_id": "APP-2025-00123",
                "score": 720,
                "decision": "APROVADO",
                "score_band": "ALTO",
                "reason": "Score 720 acima do threshold...",
                "latency_ms": 95
            }
        }
    """
    start_time = time.time()

    # Suporta invocação direta e via API Gateway
    if "body" in event and isinstance(event["body"], str):
        body = json.loads(event["body"])
    elif "body" in event and isinstance(event["body"], dict):
        body = event["body"]
    else:
        body = event

    application_id = body.get("application_id")
    cpf_hash       = body.get("cpf_hash", "").strip()

    # Validação de entrada
    if not application_id:
        return {"statusCode": 400, "body": json.dumps({"error": "'application_id' obrigatório"})}
    if not cpf_hash or len(cpf_hash) != 64:
        return {"statusCode": 400, "body": json.dumps({"error": "'cpf_hash' deve ser SHA-256 (64 chars)"})}

    logger.info(f"Score request | app={application_id} | cpf={cpf_hash[:12]}***")

    try:
        # 1. Busca features no Feature Store Online
        t1 = time.time()
        raw_features = get_features_online(cpf_hash)
        feature_latency = int((time.time() - t1) * 1000)

        if raw_features is None:
            logger.warning(f"CPF não encontrado no Feature Store | app={application_id}")
            return {
                "statusCode": 404,
                "body": json.dumps({
                    "application_id": application_id,
                    "error": "Cliente não encontrado na base de features",
                    "action": "Execute a ingestão de features para este CPF antes do scoring",
                })
            }

        # 2. Constrói vetor de features na ordem correta
        feature_vector = build_feature_vector(raw_features)

        # 3. Invoca o modelo
        t2 = time.time()
        model_result = invoke_model(feature_vector)
        model_latency = int((time.time() - t2) * 1000)

        score = model_result["score"]

        # 4. Aplica política de crédito
        decision = apply_credit_policy(score)

        # 5. Total de latência
        total_latency = int((time.time() - start_time) * 1000)

        # 6. Auditoria assíncrona (não bloqueia resposta)
        try:
            save_audit_record(
                application_id, cpf_hash, score,
                decision, feature_vector, total_latency
            )
        except Exception as e:
            # Auditoria nunca deve derrubar o scoring
            logger.error(f"Falha na auditoria DynamoDB: {e}")

        # 7. Métricas
        try:
            publish_score_metrics(score, decision["decision"], total_latency)
        except Exception:
            pass  # Métricas não são críticas

        logger.info(
            f"Score concluído | app={application_id} | score={score} | "
            f"decision={decision['decision']} | latência={total_latency}ms "
            f"(features={feature_latency}ms, model={model_latency}ms)"
        )

        response_body = {
            "application_id": application_id,
            "score":          score,
            "decision":       decision["decision"],
            "score_band":     decision["score_band"],
            "reason":         decision["reason"],
            "latency_ms":     total_latency,
            "scored_at":      datetime.utcnow().isoformat() + "Z",
        }

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(response_body),
        }

    except ClientError as e:
        logger.exception(f"Erro AWS | app={application_id}: {e}")
        return {"statusCode": 502, "body": json.dumps({"error": "Erro ao acessar serviços AWS"})}

    except Exception as e:
        logger.exception(f"Erro inesperado | app={application_id}: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": "Erro interno do servidor"})}
