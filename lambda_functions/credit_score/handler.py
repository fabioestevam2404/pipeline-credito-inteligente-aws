"""
Lambda: Scoring de Credito em Tempo Real
SLA: < 200ms (P99)
Trigger: API Gateway POST /score
"""
import json, time, boto3
from datetime import datetime, timezone

sm_runtime = boto3.client("sagemaker-runtime")
fs_runtime = boto3.client("sagemaker-featurestore-runtime")
dynamodb   = boto3.resource("dynamodb")
cw         = boto3.client("cloudwatch")

FEATURE_GROUP     = __import__("os").environ["FEATURE_GROUP"]
ENDPOINT_NAME     = __import__("os").environ["ENDPOINT_NAME"]
AUDIT_TABLE       = __import__("os").environ["AUDIT_TABLE"]
ENV               = __import__("os").environ["ENVIRONMENT"]

# Ordem exata das features — deve ser identica ao treino do modelo
FEATURE_ORDER = [
    "faixa_renda_log",
    "dias_relacionamento",
    "score_externo_norm",
    "num_restricoes",
    "flag_inadimplente",
    "divida_total",
    "qtd_tx_90d",
    "volume_tx_90d",
    "ticket_medio_90d",
    "stddev_tx_90d",
    "meses_ativos_90d",
    "ratio_credito",
    "vol_credito_90d",
    "flag_ativo",
    "maior_atraso",
]

# Politica de decisao — configuravel por negocio
SCORE_THRESHOLDS = {
    "APROVADO"       : 700,
    "ANALISE_MANUAL" : 500,
    # Abaixo de 500 = NEGADO
}


def get_features_online(cpf_hash: str) -> list:
    """Busca features do Feature Store Online. Latencia tipica < 10ms."""
    try:
        r = fs_runtime.get_record(
            FeatureGroupName=FEATURE_GROUP,
            RecordIdentifierValueAsString=cpf_hash
        )
        feat_map = {f["FeatureName"]: float(f["ValueAsString"])
                    for f in r.get("Record", [])}
        return [feat_map.get(f, 0.0) for f in FEATURE_ORDER]
    except Exception as e:
        print(f"[Score] ERRO Feature Store: {e}")
        # Fallback conservador: features zeradas
        return [0.0] * len(FEATURE_ORDER)


def invoke_model(features: list) -> dict:
    """Invoca o SageMaker endpoint e retorna score normalizado (0-1000)."""
    payload = json.dumps({"instances": [features]})

    r = sm_runtime.invoke_endpoint(
        EndpointName = ENDPOINT_NAME,
        ContentType  = "application/json",
        Body         = payload
    )
    result      = json.loads(r["Body"].read())
    probability = result["predictions"][0]["probability"]  # Prob. de inadimplencia
    # Score inversamente proporcional ao risco: prob alta = score baixo
    score = int((1 - probability) * 1000)
    return {"score": score, "probability_default": round(probability, 4)}


def apply_credit_policy(score: int) -> str:
    """Aplica a politica de credito com base no score."""
    if score >= SCORE_THRESHOLDS["APROVADO"]:
        return "APROVADO"
    elif score >= SCORE_THRESHOLDS["ANALISE_MANUAL"]:
        return "ANALISE_MANUAL"
    return "NEGADO"


def save_audit_record(application_id: str, cpf_hash: str,
                      score: int, decision: str, features: list, latency_ms: int):
    """Salva snapshot da decisao para auditoria regulatoria (BCB/LGPD)."""
    table = dynamodb.Table(AUDIT_TABLE)
    now   = datetime.now(timezone.utc)

    table.put_item(Item={
        "application_id"  : application_id,
        "decision_ts"     : now.isoformat(),
        "cpf_hash"        : cpf_hash,
        "score"           : score,
        "decision"        : decision,
        "features_snapshot": {k: str(v) for k, v in zip(FEATURE_ORDER, features)},
        "model_endpoint"  : ENDPOINT_NAME,
        "model_version"   : "v1",
        "latency_ms"      : latency_ms,
        "environment"     : ENV,
        # TTL: 5 anos (exigido pelo Banco Central)
        "ttl"             : int(now.timestamp()) + (5 * 365 * 24 * 3600)
    })


def lambda_handler(event, context):
    """
    Input:
      POST /score
      Body: { "application_id": "APP-001", "cpf_hash": "abc123..." }

    Output:
      { "score": 720, "decision": "APROVADO", "latency_ms": 85 }
    """
    start = time.time()

    # Suporta tanto chamada direta quanto via API Gateway
    if "body" in event:
        body = json.loads(event["body"]) if isinstance(event["body"], str) else event["body"]
    else:
        body = event

    application_id = body.get("application_id")
    cpf_hash       = body.get("cpf_hash")

    if not application_id or not cpf_hash:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "application_id e cpf_hash sao obrigatorios"})
        }

    print(f"[Score] application_id={application_id}")

    # 1. Busca features (<10ms)
    t1       = time.time()
    features = get_features_online(cpf_hash)
    fs_ms    = int((time.time() - t1) * 1000)

    # 2. Invoca modelo (<100ms)
    t2        = time.time()
    model_out = invoke_model(features)
    model_ms  = int((time.time() - t2) * 1000)

    score    = model_out["score"]
    decision = apply_credit_policy(score)

    # 3. Auditoria (async — nao bloqueia resposta)
    total_ms = int((time.time() - start) * 1000)
    try:
        save_audit_record(application_id, cpf_hash, score, decision, features, total_ms)
    except Exception as e:
        print(f"[Score] WARN: falha ao salvar auditoria: {e}")

    # 4. Metricas
    cw.put_metric_data(
        Namespace="CreditPipeline/Custom",
        MetricData=[
            {"MetricName": "ScoreLatencyMs", "Value": total_ms, "Unit": "Milliseconds",
             "Dimensions": [{"Name": "Environment", "Value": ENV}]},
            {"MetricName": f"Decision_{decision}", "Value": 1, "Unit": "Count",
             "Dimensions": [{"Name": "Environment", "Value": ENV}]},
        ]
    )

    print(f"[Score] score={score} decision={decision} total={total_ms}ms "
          f"(fs={fs_ms}ms model={model_ms}ms)")

    response_body = {
        "application_id"    : application_id,
        "score"             : score,
        "decision"          : decision,
        "probability_default": model_out["probability_default"],
        "latency_ms"        : total_ms
    }

    return {
        "statusCode": 200,
        "headers"   : {
            "Content-Type"                : "application/json",
            "X-Latency-Ms"                : str(total_ms),
            "Strict-Transport-Security"   : "max-age=31536000"
        },
        "body": json.dumps(response_body)
    }
