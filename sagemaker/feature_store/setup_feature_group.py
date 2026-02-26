"""
SageMaker Feature Store — Setup e Ingestao Batch
Execute UMA vez para criar o Feature Group.
Para ingestao diaria, use ingest_features.py (chamado pelo Step Functions).
"""
import boto3
import sagemaker
import pandas as pd
from datetime import datetime

sess   = sagemaker.Session()
region = sess.boto_region_name
role   = sagemaker.get_execution_role()

# ── Definicao das Features ─────────────────────────────────────────────────
FEATURE_GROUP_NAME = "credit-features-v1"

FEATURE_DEFINITIONS = [
    # Identificadores
    {"FeatureName": "cpf_hash",            "FeatureType": "String"},
    # Features cadastrais
    {"FeatureName": "faixa_etaria",        "FeatureType": "String"},
    {"FeatureName": "renda_padronizada",   "FeatureType": "Fractional"},
    {"FeatureName": "faixa_renda_log",     "FeatureType": "Integral"},
    {"FeatureName": "dias_relacionamento", "FeatureType": "Integral"},
    {"FeatureName": "status_cliente",      "FeatureType": "String"},
    {"FeatureName": "flag_ativo",          "FeatureType": "Integral"},
    # Features do bureau
    {"FeatureName": "score_externo",       "FeatureType": "Fractional"},
    {"FeatureName": "score_externo_norm",  "FeatureType": "Fractional"},
    {"FeatureName": "num_restricoes",      "FeatureType": "Integral"},
    {"FeatureName": "divida_total",        "FeatureType": "Fractional"},
    {"FeatureName": "flag_inadimplente",   "FeatureType": "Integral"},
    {"FeatureName": "maior_atraso",        "FeatureType": "Integral"},
    # Features transacionais
    {"FeatureName": "qtd_tx_90d",          "FeatureType": "Integral"},
    {"FeatureName": "volume_tx_90d",       "FeatureType": "Fractional"},
    {"FeatureName": "ticket_medio_90d",    "FeatureType": "Fractional"},
    {"FeatureName": "stddev_tx_90d",       "FeatureType": "Fractional"},
    {"FeatureName": "meses_ativos_90d",    "FeatureType": "Integral"},
    {"FeatureName": "ratio_credito",       "FeatureType": "Fractional"},
    {"FeatureName": "vol_credito_90d",     "FeatureType": "Fractional"},
    # Metadados
    {"FeatureName": "feature_ts",          "FeatureType": "String"},
    {"FeatureName": "feature_date",        "FeatureType": "String"},
    {"FeatureName": "feature_group",       "FeatureType": "String"},
]

def create_feature_group(gold_bucket: str, env: str):
    """Cria o Feature Group no SageMaker Feature Store."""
    sm_client = boto3.client("sagemaker", region_name=region)

    print(f"Criando Feature Group: {FEATURE_GROUP_NAME}")

    sm_client.create_feature_group(
        FeatureGroupName           = FEATURE_GROUP_NAME,
        RecordIdentifierFeatureName = "cpf_hash",
        EventTimeFeatureName        = "feature_ts",
        FeatureDefinitions          = FEATURE_DEFINITIONS,
        OnlineStoreConfig  = {
            "EnableOnlineStore": True,  # Acesso <10ms para scoring
            "SecurityConfig"   : {
                "KmsKeyId": f"alias/credit-pipeline-{env}"
            }
        },
        OfflineStoreConfig = {
            "S3StorageConfig": {
                "S3Uri"    : f"s3://{gold_bucket}/feature-store/",
                "KmsKeyId" : f"alias/credit-pipeline-{env}"
            },
            "DisableGlueTableCreation": False,  # Cataloga automaticamente
            "DataCatalogConfig": {
                "TableName" : "credit_features",
                "Catalog"   : "AwsDataCatalog",
                "Database"  : f"credit_db_{env}"
            }
        },
        RoleArn = role,
        Tags    = [
            {"Key": "Environment", "Value": env},
            {"Key": "Team",        "Value": "data-engineering"},
        ]
    )

    print(f"Feature Group {FEATURE_GROUP_NAME} criado com sucesso!")
    return FEATURE_GROUP_NAME


if __name__ == "__main__":
    import sys
    gold_bucket = sys.argv[1] if len(sys.argv) > 1 else "credit-pipeline-gold"
    env         = sys.argv[2] if len(sys.argv) > 2 else "prod"
    create_feature_group(gold_bucket, env)
