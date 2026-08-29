"""
Glue Job: Feature Engineering
Camada: Gold — calcula features para o modelo de credito
"""
import sys
from datetime import datetime as _datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "run_date", "SILVER_BUCKET", "GOLD_BUCKET", "ENVIRONMENT"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RUN_DATE      = args["run_date"]
SILVER_BUCKET = args["SILVER_BUCKET"]
GOLD_BUCKET   = args["GOLD_BUCKET"]
ENV           = args["ENVIRONMENT"]

# Valida o formato antes de interpolar em SQL — RUN_DATE vem de um parâmetro
# de job (Step Functions/EventBridge), não de input de usuário, mas isso
# garante que só uma data bem formada chega até a query (ver bandit B608).
_datetime.strptime(RUN_DATE, "%Y-%m-%d")

# ── Leitura das 3 fontes Silver ────────────────────────────────────────────
print("[Features] Lendo dados Silver...")

df_clientes = spark.sql(f"""
    SELECT * FROM glue_catalog.credit_db_{ENV}.clientes_silver
    WHERE processing_date = '{RUN_DATE}'
""")

df_bureau = spark.read.parquet(
    f"s3://{SILVER_BUCKET}/bureau/year={RUN_DATE[:4]}/month={RUN_DATE[5:7]}/day={RUN_DATE[8:10]}/"
)

cutoff_90d = F.date_sub(F.lit(RUN_DATE).cast("date"), 90)
df_tx = (spark.read.parquet(f"s3://{SILVER_BUCKET}/transactions/")
    .filter(F.to_date("event_time") >= cutoff_90d))

# ── Features Cadastrais ────────────────────────────────────────────────────
feat_cadastro = df_clientes.select(
    "cpf_hash",
    "faixa_etaria",
    "renda_padronizada",
    "faixa_renda_log",
    "dias_relacionamento",
    "status_cliente",
    F.when(F.col("status_cliente") == "ATIVO", 1).otherwise(0).alias("flag_ativo"),
)

# ── Features do Bureau ─────────────────────────────────────────────────────
feat_bureau = df_bureau.select(
    "cpf_hash",
    F.col("score_bureau").alias("score_externo"),
    (F.col("score_bureau") / 1000.0).alias("score_externo_norm"),
    F.col("qtd_restricoes").alias("num_restricoes"),
    F.col("valor_total_dividas").alias("divida_total"),
    F.when(F.col("qtd_restricoes") > 0, 1).otherwise(0).alias("flag_inadimplente"),
    F.col("maior_atraso_dias").alias("maior_atraso"),
)

# ── Features Transacionais (90 dias) ──────────────────────────────────────
feat_tx = df_tx.groupBy("client_id").agg(
    F.count("*")                          .alias("qtd_tx_90d"),
    F.sum("amount")                       .alias("volume_tx_90d"),
    F.avg("amount")                       .alias("ticket_medio_90d"),
    F.max("amount")                       .alias("maior_tx_90d"),
    F.stddev("amount")                    .alias("stddev_tx_90d"),
    F.countDistinct(F.date_trunc("month", F.col("event_time"))).alias("meses_ativos_90d"),
    F.avg(F.when(F.col("tx_type") == "CREDITO", 1.0).otherwise(0.0)).alias("ratio_credito"),
    F.sum(F.when(F.col("tx_type") == "CREDITO", F.col("amount")).otherwise(0)).alias("vol_credito_90d"),
)

# ── Join Final ─────────────────────────────────────────────────────────────
features = (feat_cadastro
    .join(feat_bureau, "cpf_hash", "left")
    .join(feat_tx, feat_cadastro.cpf_hash == feat_tx.client_id, "left")
    .withColumn("feature_ts",    F.current_timestamp())
    .withColumn("feature_date",  F.lit(RUN_DATE))
    .withColumn("feature_group", F.lit("credit_features_v1"))
    .fillna({
        "score_externo"    : 0,
        "score_externo_norm": 0.0,
        "num_restricoes"   : 0,
        "divida_total"     : 0.0,
        "flag_inadimplente": 1,      # Conservador: sem dado = inadimplente
        "maior_atraso"     : 0,
        "qtd_tx_90d"       : 0,
        "volume_tx_90d"    : 0.0,
        "ticket_medio_90d" : 0.0,
        "stddev_tx_90d"    : 0.0,
        "meses_ativos_90d" : 0,
        "ratio_credito"    : 0.0,
        "vol_credito_90d"  : 0.0,
    })
)

feature_count = features.count()
print(f"[Features] {feature_count} vetores de features calculados")

# ── Grava no S3 Gold ───────────────────────────────────────────────────────
gold_path = (f"s3://{GOLD_BUCKET}/features/"
             f"year={RUN_DATE[:4]}/month={RUN_DATE[5:7]}/day={RUN_DATE[8:10]}/")

(features.write.mode("overwrite")
    .option("compression", "snappy")
    .parquet(gold_path))

print(f"[Features] Gravado em {gold_path}")
print("[Features] Proxima etapa: ingest_features.py no SageMaker Feature Store")
job.commit()
