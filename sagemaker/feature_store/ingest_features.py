"""
SageMaker Processing Job: Ingestao de Features no Feature Store
Chamado pelo Step Functions diariamente apos o Feature Engineering job.
"""
import os, sys, boto3, argparse
import pandas as pd
from datetime import datetime

# Argumentos passados pelo Step Functions
parser = argparse.ArgumentParser()
parser.add_argument("--run-date",    required=True)
parser.add_argument("--gold-bucket", required=True)
parser.add_argument("--env",         default="prod")
args = parser.parse_args()

RUN_DATE    = args.run_date
GOLD_BUCKET = args.gold_bucket
ENV         = args.env

print(f"[FeatureStore] Ingestao para run_date={RUN_DATE}")

# Lê features do S3 Gold
s3_path = (f"s3://{GOLD_BUCKET}/features/"
           f"year={RUN_DATE[:4]}/month={RUN_DATE[5:7]}/day={RUN_DATE[8:10]}/")

import pyarrow.parquet as pq
import s3fs

fs = s3fs.S3FileSystem()
dataset = pq.read_table(s3_path, filesystem=fs)
df      = dataset.to_pandas()

# Converte timestamp para string ISO8601 (exigido pelo Feature Store)
df["feature_ts"] = df["feature_ts"].astype(str)

total = len(df)
print(f"[FeatureStore] {total} registros a ingerir")

# Ingestao em batches para evitar throttling
sm_runtime = boto3.client("sagemaker-featurestore-runtime")
BATCH_SIZE  = 500
errors      = 0

for i in range(0, total, BATCH_SIZE):
    batch = df.iloc[i:i+BATCH_SIZE]

    for _, row in batch.iterrows():
        try:
            sm_runtime.put_record(
                FeatureGroupName = "credit-features-v1",
                Record = [
                    {"FeatureName": col, "ValueAsString": str(val)}
                    for col, val in row.items()
                    if pd.notna(val)
                ]
            )
        except Exception as e:
            errors += 1
            print(f"[FeatureStore] ERRO ao inserir {row['cpf_hash'][:8]}: {e}")

    pct = min(i + BATCH_SIZE, total) / total * 100
    print(f"[FeatureStore] {pct:.0f}% concluido ({min(i+BATCH_SIZE, total)}/{total})")

print(f"[FeatureStore] Concluido: {total - errors} OK, {errors} erros")

if errors > total * 0.01:  # Falha se mais de 1% com erro
    raise RuntimeError(f"Taxa de erro na ingestao: {errors}/{total}")
