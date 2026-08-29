"""
Glue Job: Ingestão Incremental do CRM
Camada: Bronze (Raw)
Frequência: Diária via Step Functions
"""
import json
import sys
from datetime import datetime

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "run_date", "RAW_BUCKET", "ENVIRONMENT"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RUN_DATE   = args["run_date"]
RAW_BUCKET = args["RAW_BUCKET"]
ENV        = args["ENVIRONMENT"]

# Valida o formato antes de interpolar na query JDBC — o conector JDBC do
# Spark não aceita query parametrizada em .option("query", ...), então a
# validação estrita é a mitigação real aqui (ver bandit B608).
datetime.strptime(RUN_DATE, "%Y-%m-%d")


def get_secret(name):
    client = boto3.client("secretsmanager")
    r = client.get_secret_value(SecretId=name)
    return json.loads(r["SecretString"]).get("password", "")


def publish_metric(name, value, unit="Count"):
    boto3.client("cloudwatch").put_metric_data(
        Namespace="CreditPipeline/Custom",
        MetricData=[{
            "MetricName": name, "Value": value, "Unit": unit,
            "Dimensions": [{"Name": "Environment", "Value": ENV},
                           {"Name": "RunDate", "Value": RUN_DATE}]
        }]
    )


print(f"[IngestCRM] run_date={RUN_DATE}")

jdbc_url = "jdbc:postgresql://crm-db.internal:5432/credit_db"

query = f"""
    SELECT client_id, cpf_hash, nome_completo, data_nascimento,
           renda_declarada, data_abertura_conta, status_cliente,
           limite_aprovado, segmento, uf, updated_at, created_at
    FROM clientes
    WHERE updated_at::date = '{RUN_DATE}'::date
"""

df_raw = (spark.read
    .format("jdbc")
    .option("url",           jdbc_url)
    .option("query",         query)
    .option("driver",        "org.postgresql.Driver")
    .option("user",          "glue_reader")
    .option("password",      get_secret("credit-pipeline/crm-db"))
    .option("fetchsize",     "10000")
    .option("numPartitions", "8")
    .option("partitionColumn", "client_id")
    .option("lowerBound",    "1")
    .option("upperBound",    "10000000")
    .load())

raw_count = df_raw.count()
print(f"[IngestCRM] {raw_count} registros lidos")

if raw_count == 0:
    print("[IngestCRM] WARN: Nenhum registro. Verificar fonte.")
    publish_metric("RecordsIngested", 0)
    job.commit()
    sys.exit(0)

# Enriquece com metadados de ingestão
df_out = (df_raw
    .withColumn("ingestion_ts",     F.current_timestamp())
    .withColumn("ingestion_date",   F.lit(RUN_DATE))
    .withColumn("source_system",    F.lit("crm_postgres"))
    .withColumn("pipeline_version", F.lit("1.0.0")))

# Grava particionado por data
run_dt   = datetime.strptime(RUN_DATE, "%Y-%m-%d")
out_path = (f"s3://{RAW_BUCKET}/crm/clientes/"
            f"year={run_dt.year}/month={run_dt.month:02d}/day={run_dt.day:02d}/")

(df_out.write.mode("overwrite")
    .option("compression", "snappy")
    .parquet(out_path))

publish_metric("RecordsIngested", raw_count)
print(f"[IngestCRM] Concluido: {raw_count} registros em {out_path}")
job.commit()
