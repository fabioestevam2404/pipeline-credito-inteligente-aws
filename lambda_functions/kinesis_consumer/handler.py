"""
Lambda: Kinesis Consumer — Transacoes em Tempo Real
Trigger: Kinesis Data Stream (batch de ate 500 registros)
"""
import base64
import json
from datetime import datetime, timezone

import boto3

s3  = boto3.client("s3")
cw  = boto3.client("cloudwatch")

RAW_BUCKET = __import__("os").environ["RAW_BUCKET"]
KMS_KEY_ID = __import__("os").environ["KMS_KEY_ID"]
ENV        = __import__("os").environ["ENVIRONMENT"]

REQUIRED_FIELDS = {"client_id", "amount", "tx_type", "tx_id"}


def validate_record(data: dict) -> tuple:
    """Valida os campos obrigatorios. Retorna (valido, motivo)."""
    missing = REQUIRED_FIELDS - set(data.keys())
    if missing:
        return False, f"campos_faltando:{missing}"
    if not isinstance(data.get("amount"), (int, float)) or data["amount"] < 0:
        return False, "amount_invalido"
    if data.get("tx_type") not in ("CREDITO", "DEBITO", "PIX", "TED"):
        return False, f"tx_type_invalido:{data.get('tx_type')}"
    return True, None


def lambda_handler(event, context):
    """Processa batch do Kinesis e salva no S3 particionado por hora."""
    records_ok     = []
    records_failed = []

    for record in event["Records"]:
        try:
            payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
            data    = json.loads(payload)
        except Exception as e:
            print(f"[Kinesis] ERRO ao decodificar: {e}")
            records_failed.append({"raw": payload[:200], "error": str(e)})
            continue

        valid, reason = validate_record(data)
        if not valid:
            records_failed.append({"tx_id": data.get("tx_id"), "reason": reason})
            continue

        records_ok.append({
            **data,
            "event_time": record["kinesis"]["approximateArrivalTimestamp"],
            "kinesis_seq": record["kinesis"]["sequenceNumber"],
            "ingested_at": datetime.now(timezone.utc).isoformat()
        })

    if records_ok:
        now    = datetime.now(timezone.utc)
        s3_key = (f"transactions/"
                  f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
                  f"hour={now.hour:02d}/batch_{int(now.timestamp())}.json")

        s3.put_object(
            Bucket      = RAW_BUCKET,
            Key         = s3_key,
            Body        = "\n".join(json.dumps(r, ensure_ascii=False) for r in records_ok),
            ContentType = "application/x-ndjson",
            ServerSideEncryption = "aws:kms",
            SSEKMSKeyId = KMS_KEY_ID
        )

    # Publica metricas
    cw.put_metric_data(
        Namespace="CreditPipeline/Custom",
        MetricData=[
            {"MetricName": "KinesisRecordsOK",     "Value": len(records_ok),     "Unit": "Count",
             "Dimensions": [{"Name": "Environment", "Value": ENV}]},
            {"MetricName": "KinesisRecordsFailed",  "Value": len(records_failed), "Unit": "Count",
             "Dimensions": [{"Name": "Environment", "Value": ENV}]},
        ]
    )

    if records_failed:
        print(f"[Kinesis] WARN: {len(records_failed)} registros invalidos")
        for f in records_failed[:5]:  # Loga apenas os primeiros 5
            print(f"  -> {f}")

    print(f"[Kinesis] OK={len(records_ok)} FAIL={len(records_failed)}")
    return {"batchItemFailures": [], "processed": len(records_ok)}
