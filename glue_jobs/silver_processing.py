"""
Glue Job: Processamento Silver (Limpeza + Iceberg UPSERT)
Camada: Silver
"""
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "run_date", "RAW_BUCKET", "SILVER_BUCKET", "ENVIRONMENT"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RUN_DATE      = args["run_date"]
RAW_BUCKET    = args["RAW_BUCKET"]
SILVER_BUCKET = args["SILVER_BUCKET"]
ENV           = args["ENVIRONMENT"]

# Configura Iceberg
spark.conf.set("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.glue_catalog",
    "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse",
    f"s3://{SILVER_BUCKET}/")

from datetime import datetime
run_dt = datetime.strptime(RUN_DATE, "%Y-%m-%d")
raw_path = f"s3://{RAW_BUCKET}/crm/clientes/year={run_dt.year}/month={run_dt.month:02d}/day={run_dt.day:02d}/"

print(f"[Silver] Lendo de {raw_path}")
df_raw = spark.read.parquet(raw_path)
total  = df_raw.count()
print(f"[Silver] {total} registros brutos")

# ── Validação de Qualidade ─────────────────────────────────────────────────
df_valid = (df_raw
    .filter(F.col("cpf_hash").isNotNull())
    .filter(F.col("renda_declarada").isNull() | (F.col("renda_declarada") >= 0))
    .filter(F.col("data_nascimento").between("1905-01-01", "2007-12-31"))
    .filter(F.col("status_cliente").isin(["ATIVO","INATIVO","BLOQUEADO","ENCERRADO"]))
)

dropped = total - df_valid.count()
drop_pct = dropped / max(total, 1) * 100
print(f"[Silver] {dropped} registros invalidos ({drop_pct:.1f}%)")

if drop_pct > 20:
    raise ValueError(f"Taxa de descarte critica: {drop_pct:.1f}% > 20%. Pipeline interrompido.")

# ── Transformacoes ─────────────────────────────────────────────────────────
df_clean = (df_valid
    .withColumn("nome_normalizado",
        F.upper(F.trim(F.col("nome_completo"))))
    .withColumn("faixa_etaria",
        F.when(F.datediff(F.current_date(), F.col("data_nascimento")) < 365*25, "18-24")
         .when(F.datediff(F.current_date(), F.col("data_nascimento")) < 365*35, "25-34")
         .when(F.datediff(F.current_date(), F.col("data_nascimento")) < 365*50, "35-49")
         .when(F.datediff(F.current_date(), F.col("data_nascimento")) < 365*65, "50-64")
         .otherwise("65+"))
    .withColumn("renda_padronizada",
        F.coalesce(F.col("renda_declarada").cast("double"), F.lit(-1.0)))
    .withColumn("faixa_renda_log",
        F.when(F.col("renda_padronizada") <= 0,     0)
         .when(F.col("renda_padronizada") <= 1500,  1)
         .when(F.col("renda_padronizada") <= 3000,  2)
         .when(F.col("renda_padronizada") <= 7000,  3)
         .when(F.col("renda_padronizada") <= 15000, 4)
         .otherwise(5))
    .withColumn("dias_relacionamento",
        F.datediff(F.current_date(), F.col("data_abertura_conta")))
    .withColumn("processing_date", F.lit(RUN_DATE))
    .withColumn("processing_ts",   F.current_timestamp())
)

# ── Deduplicacao ───────────────────────────────────────────────────────────
window = Window.partitionBy("cpf_hash").orderBy(F.desc("updated_at"))
df_dedup = (df_clean
    .withColumn("row_num", F.row_number().over(window))
    .filter(F.col("row_num") == 1)
    .drop("row_num"))

final_count = df_dedup.count()
print(f"[Silver] {final_count} registros unicos apos deduplicacao")

# ── UPSERT no Iceberg ──────────────────────────────────────────────────────
df_dedup.createOrReplaceTempView("updates")

# Cria tabela se nao existir
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.credit_db_{ENV}.clientes_silver
    USING iceberg
    PARTITIONED BY (processing_date)
    LOCATION 's3://{SILVER_BUCKET}/clientes_silver/'
    AS SELECT * FROM updates WHERE 1=0
""")

spark.sql(f"""
    MERGE INTO glue_catalog.credit_db_{ENV}.clientes_silver AS t
    USING updates AS s ON t.cpf_hash = s.cpf_hash
    WHEN MATCHED THEN UPDATE SET
        nome_normalizado    = s.nome_normalizado,
        renda_padronizada   = s.renda_padronizada,
        faixa_renda_log     = s.faixa_renda_log,
        faixa_etaria        = s.faixa_etaria,
        status_cliente      = s.status_cliente,
        dias_relacionamento = s.dias_relacionamento,
        processing_date     = s.processing_date,
        processing_ts       = s.processing_ts
    WHEN NOT MATCHED THEN INSERT *
""")

import boto3
boto3.client("cloudwatch").put_metric_data(
    Namespace="CreditPipeline/Custom",
    MetricData=[
        {"MetricName": "RecordsProcessed", "Value": final_count, "Unit": "Count",
         "Dimensions": [{"Name": "Environment", "Value": ENV}]},
        {"MetricName": "RecordsDropped", "Value": dropped, "Unit": "Count",
         "Dimensions": [{"Name": "Environment", "Value": ENV}]},
        {"MetricName": "DataDropRate", "Value": drop_pct, "Unit": "Percent",
         "Dimensions": [{"Name": "Environment", "Value": ENV}]},
    ]
)

print(f"[Silver] Concluido: {final_count} registros no Iceberg")
job.commit()
