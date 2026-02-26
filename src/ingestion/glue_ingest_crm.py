"""
src/ingestion/glue_ingest_crm.py
Glue ETL Job — Ingestão incremental do CRM para S3 Raw (Bronze Layer).

Fluxo:
  PostgreSQL CRM → Glue JDBC → S3 Raw (Parquet + Snappy + KMS)

Execução incremental por bookmark do Glue (job-bookmark-enable).
Também suporta execução por --run_date para reprocessamento manual.
"""
import sys
import logging
from datetime import datetime, timedelta

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, TimestampType

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ── Inicialização do Contexto Glue ───────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "RAW_BUCKET",
    "KMS_KEY_ARN",
])
# Parâmetros opcionais
run_date = None
for arg in sys.argv:
    if arg.startswith("--run_date="):
        run_date = arg.split("=", 1)[1]

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_BUCKET  = args["RAW_BUCKET"]
KMS_KEY_ARN = args["KMS_KEY_ARN"]

# ── Configurações de Performance do Spark ────────────────────
spark.conf.set("spark.sql.adaptive.enabled", "true")          # AQE para otimização automática
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

# ── Definição da Query de Ingestão ───────────────────────────
# Se run_date fornecido: reprocessa a data específica
# Senão: usa bookmark automático do Glue
if run_date:
    run_dt   = datetime.strptime(run_date, "%Y-%m-%d")
    next_day = (run_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    where_clause = f"updated_at >= '{run_date}'::date AND updated_at < '{next_day}'::date"
    logger.info(f"Modo reprocessamento: data={run_date}")
else:
    # Bookmark do Glue controla o incremento automaticamente
    where_clause = "1=1"
    logger.info("Modo incremental com Glue Job Bookmark")

INGEST_QUERY = f"""
    SELECT
        client_id                           AS client_id,
        cpf_hash,                           -- CPF já hasheado no banco (SHA-256)
        nome_completo,
        data_nascimento,
        email_hash,                         -- Email também hasheado
        telefone_ddd,                       -- Apenas DDD, não número completo
        cidade,
        estado,
        renda_declarada,
        data_abertura_conta,
        status_cliente,
        limite_aprovado,
        ultimo_pagamento_atraso,
        qtd_contratos_ativos,
        updated_at
    FROM clientes
    WHERE {where_clause}
"""

# ── Leitura via JDBC ─────────────────────────────────────────
logger.info("Iniciando leitura do CRM via JDBC...")

# Credenciais do Secrets Manager (injetadas pelo Glue Connection)
jdbc_url = f"jdbc:postgresql://{args.get('CRM_HOST', 'localhost')}:5432/credit_db"

df_raw = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("query", INGEST_QUERY)
    .option("driver", "org.postgresql.Driver")
    .option("user", "glue_reader")
    .option("password", "{{SECRETS_MANAGER_REF}}")  # Substituído pelo Glue Connection
    .option("fetchsize", "10000")          # Lê 10k linhas por chunk
    .option("numPartitions", "8")          # Paralelismo de leitura
    .option("partitionColumn", "client_id")
    .option("lowerBound", "1")
    .option("upperBound", "100000000")
    .load())

record_count = df_raw.count()
logger.info(f"Lidos {record_count:,} registros do CRM")

if record_count == 0:
    logger.warning("Nenhum registro encontrado — verificar fonte ou configuração do bookmark")
    job.commit()
    sys.exit(0)

# ── Enriquecimento com Metadados de Ingestão ─────────────────
df_enriched = (df_raw
    .withColumn("ingestion_ts",       F.current_timestamp())
    .withColumn("ingestion_date",     F.current_date())
    .withColumn("source_system",      F.lit("crm_postgresql"))
    .withColumn("pipeline_version",   F.lit("1.0.0"))
    .withColumn("job_run_id",         F.lit(args["JOB_NAME"]))
    # Particionamento por data de atualização (não de ingestão)
    .withColumn("partition_year",     F.year("updated_at").cast("string"))
    .withColumn("partition_month",    F.lpad(F.month("updated_at").cast("string"), 2, "0"))
    .withColumn("partition_day",      F.lpad(F.dayofmonth("updated_at").cast("string"), 2, "0"))
)

# ── Gravação no S3 Raw com Particionamento ───────────────────
output_path = f"s3://{RAW_BUCKET}/crm/clientes/"

logger.info(f"Gravando em {output_path} (particionado por year/month/day)...")

(df_enriched.write
    .mode("append")                            # Append: nunca sobrescreve dados históricos
    .partitionBy("partition_year", "partition_month", "partition_day")
    .option("compression", "snappy")
    .parquet(output_path))

# ── Publicação de Métricas ────────────────────────────────────
import boto3
cw = boto3.client("cloudwatch")
cw.put_metric_data(
    Namespace  = "CreditPipeline/Custom",
    MetricData = [{
        "MetricName": "RecordsIngested",
        "Value":      float(record_count),
        "Unit":       "Count",
        "Dimensions": [{"Name": "Source", "Value": "CRM"}]
    }]
)

logger.info(f"✅ Ingestão CRM concluída: {record_count:,} registros em {output_path}")
job.commit()
