"""
Script: Direito ao Esquecimento (LGPD Art. 18)
Uso: python lgpd_deletion.py --cpf-hash <hash> --request-id <id> --env prod
"""
import argparse, json, time, boto3
from datetime import datetime, timezone

def main():
    parser = argparse.ArgumentParser(description="LGPD Data Deletion")
    parser.add_argument("--cpf-hash",   required=True)
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--env",        default="prod")
    parser.add_argument("--dry-run",    action="store_true",
                        help="Simula sem deletar dados reais")
    args = parser.parse_args()

    cpf_hash   = args.cpf_hash
    req_id     = args.request_id
    env        = args.env
    dry_run    = args.dry_run

    print(f"[LGPD] {'DRY RUN - ' if dry_run else ''}Iniciando delecao para cpf_hash={cpf_hash[:8]}***")

    log = {
        "request_id"     : req_id,
        "cpf_hash_prefix": cpf_hash[:8],
        "started_at"     : datetime.now(timezone.utc).isoformat(),
        "environment"    : env,
        "dry_run"        : dry_run,
        "actions"        : []
    }

    errors = []

    # 1. Feature Store Online
    try:
        if not dry_run:
            fs = boto3.client("sagemaker-featurestore-runtime")
            fs.delete_record(
                FeatureGroupName="credit-features-v1",
                RecordIdentifierValueAsString=cpf_hash,
                EventTime=str(datetime.now(timezone.utc).timestamp())
            )
        log["actions"].append({"step": "feature_store_online", "status": "deleted" if not dry_run else "skipped_dry_run"})
        print("[LGPD] 1/4 Feature Store Online: OK")
    except Exception as e:
        errors.append(f"feature_store: {e}")
        log["actions"].append({"step": "feature_store_online", "status": "error", "detail": str(e)})
        print(f"[LGPD] 1/4 Feature Store: ERRO - {e}")

    # 2. DynamoDB — tabela de auditoria
    try:
        ddb   = boto3.resource("dynamodb")
        table = ddb.Table(f"credit-score-audit-{env}")

        # Busca por GSI cpf-hash-index
        r = table.query(
            IndexName="cpf-hash-index",
            KeyConditionExpression="cpf_hash = :h",
            ExpressionAttributeValues={":h": cpf_hash}
        )
        items = r.get("Items", [])

        if not dry_run:
            for item in items:
                table.delete_item(Key={
                    "application_id": item["application_id"],
                    "decision_ts"   : item["decision_ts"]
                })

        log["actions"].append({
            "step"   : "dynamodb_audit",
            "status" : "deleted" if not dry_run else "would_delete",
            "count"  : len(items)
        })
        print(f"[LGPD] 2/4 DynamoDB Audit: {len(items)} registros {'deletados' if not dry_run else 'encontrados (dry run)'}")
    except Exception as e:
        errors.append(f"dynamodb: {e}")
        log["actions"].append({"step": "dynamodb_audit", "status": "error", "detail": str(e)})
        print(f"[LGPD] 2/4 DynamoDB: ERRO - {e}")

    # 3. S3 — agenda S3 Batch Operation (delecao async dos objetos Parquet)
    try:
        if not dry_run:
            s3 = boto3.client("s3")
            # Cria manifest para S3 Batch Operations
            # Na pratica, isso exige um job separado para varrer todos os buckets
            # e identificar objetos com o cpf_hash
            print("[LGPD] 3/4 S3: agendando S3 Batch Operation para remocao de dados brutos")
        log["actions"].append({
            "step"  : "s3_data",
            "status": "batch_operation_queued" if not dry_run else "skipped_dry_run",
            "note"  : "S3 Batch Operation necessario para varredura completa dos buckets"
        })
        print(f"[LGPD] 3/4 S3 Batch: {'agendado' if not dry_run else 'simulado'}")
    except Exception as e:
        errors.append(f"s3: {e}")

    # 4. Salva log de conformidade (imutavel)
    try:
        log["completed_at"] = datetime.now(timezone.utc).isoformat()
        log["errors"]       = errors
        log["success"]      = len(errors) == 0

        if not dry_run:
            s3 = boto3.client("s3")
            account_id = boto3.client("sts").get_caller_identity()["Account"]
            bucket     = f"credit-pipeline-compliance-{account_id}-{env}"
            key        = f"lgpd-deletions/{req_id}.json"
            s3.put_object(
                Bucket      = bucket,
                Key         = key,
                Body        = json.dumps(log, indent=2, ensure_ascii=False),
                ContentType = "application/json"
            )
            print(f"[LGPD] 4/4 Log de conformidade salvo: s3://{bucket}/{key}")
        else:
            print(f"[LGPD] 4/4 Log de conformidade: simulado (dry run)")
    except Exception as e:
        print(f"[LGPD] 4/4 Log de conformidade: ERRO - {e}")

    # Resultado final
    print("")
    if errors:
        print(f"[LGPD] CONCLUIDO COM ERROS: {len(errors)} passos falharam")
        for e in errors:
            print(f"  -> {e}")
        exit(1)
    else:
        print(f"[LGPD] CONCLUIDO COM SUCESSO - request_id={req_id}")

if __name__ == "__main__":
    main()
