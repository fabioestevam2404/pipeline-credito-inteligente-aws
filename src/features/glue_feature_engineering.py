"""
src/features/glue_feature_engineering.py
Glue ETL Job — Feature Engineering para o modelo de crédito.

Combina 3 fontes de dados e calcula o vetor de features final:
  1. Dados cadastrais (Silver CRM)
  2. Score e restrições (Bureau/Serasa)
  3. Comportamento transacional (últimos 30/90/180 dias)

Saída: S3 Gold (Parquet) — ingerido no SageMaker Feature Store pela Lambda seguinte
"""
import sys
import logging
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window

import boto3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ── Inicialização ─────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "SILVER_BUCKET", "GOLD_BUCKET"])
run_date = next((a.split("=", 1)[1] for a in sys.argv if a.startswith("--run_date=")),
                datetime.utcnow().strftime("%Y-%m-%d"))

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_BUCKET = args["SILVER_BUCKET"]
GOLD_BUCKET   = args["GOLD_BUCKET"]

# Configura Iceberg para leitura do Silver
spark.conf.set(
    "spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
)
spark.conf.set("spark.sql.catalog.glue_catalog",   "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{SILVER_BUCKET}/")
spark.conf.set("spark.sql.adaptive.enabled", "true")


# ── LEITURA DAS FONTES ────────────────────────────────────────
logger.info("Carregando dados das 3 fontes...")

# Fonte 1: Dados cadastrais (Silver — Iceberg)
df_clientes = spark.sql("""
    SELECT
        cpf_hash,
        client_id,
        faixa_etaria,
        faixa_renda_log,
        renda_padronizada,
        dias_relacionamento,
        anos_relacionamento,
        cliente_antigo,
        status_cliente,
        limite_aprovado,
        qtd_contratos_ativos,
        updated_at AS cadastro_updated_at
    FROM glue_catalog.credit_db.clientes_silver
    WHERE status_cliente IN ('ATIVO', 'BLOQUEADO')
""")

# Fonte 2: Dados do bureau (S3 Raw — JSON normalizado)
df_bureau = (spark.read
    .json(f"s3://{SILVER_BUCKET}/bureau_consolidated/")
    .select(
        "cpf_hash",
        F.col("score_bureau").alias("score_externo"),
        F.col("qtd_restricoes").alias("num_restricoes"),
        F.col("valor_total_dividas").alias("divida_total"),
        F.col("possui_cheque_sem_fundo").alias("flag_cheque"),
        F.col("meses_negativado"),
        "queried_at"
    ))

# Fonte 3: Transações (S3 Raw — Parquet particionado)
# Carrega 180 dias para calcular janelas temporais múltiplas
cutoff_180d = F.date_sub(F.current_date(), 180)
df_transacoes = (spark.read
    .parquet(f"s3://{SILVER_BUCKET.replace('silver', 'raw')}/transactions/")
    .filter(F.to_date("event_time") >= cutoff_180d)
    .select("client_id", "amount", "tx_type", "event_time"))

logger.info(f"Clientes: {df_clientes.count():,} | "
            f"Bureau: {df_bureau.count():,} | "
            f"Transações 180d: {df_transacoes.count():,}")


# ── FEATURES TRANSACIONAIS ────────────────────────────────────
def build_tx_features(df_tx: DataFrame, window_days: int, suffix: str) -> DataFrame:
    """
    Calcula features de comportamento transacional para uma janela temporal.

    Args:
        df_tx: DataFrame de transações
        window_days: Número de dias da janela
        suffix: Sufixo para nomear as colunas (ex: "_30d", "_90d")
    """
    cutoff = F.date_sub(F.current_date(), window_days)
    df_window = df_tx.filter(F.to_date("event_time") >= cutoff)

    return df_window.groupBy("client_id").agg(
        F.count("*")                     .alias(f"qtd_tx{suffix}"),
        F.sum("amount")                  .alias(f"volume_tx{suffix}"),
        F.avg("amount")                  .alias(f"ticket_medio{suffix}"),
        F.max("amount")                  .alias(f"maior_tx{suffix}"),
        F.min("amount")                  .alias(f"menor_tx{suffix}"),
        F.stddev("amount")               .alias(f"std_tx{suffix}"),
        F.countDistinct(F.date_trunc("month", "event_time"))
                                         .alias(f"meses_ativos{suffix}"),
        # % de transações de crédito (vs débito)
        F.avg(F.when(F.col("tx_type") == "CREDITO", 1.0).otherwise(0.0))
                                         .alias(f"ratio_credito{suffix}"),
        # Maior dia de movimentação (comportamento de salário)
        F.mode(F.dayofmonth("event_time")).alias(f"dia_pico{suffix}"),
    )


feat_tx_30d  = build_tx_features(df_transacoes, 30,  "_30d")
feat_tx_90d  = build_tx_features(df_transacoes, 90,  "_90d")
feat_tx_180d = build_tx_features(df_transacoes, 180, "_180d")

# Une todas as janelas transacionais
df_tx_features = (feat_tx_30d
    .join(feat_tx_90d,  "client_id", "outer")
    .join(feat_tx_180d, "client_id", "outer"))


# ── FEATURES BUREAU ───────────────────────────────────────────
df_bureau_features = df_bureau.withColumn(
    # Score normalizado para [0, 1]
    "score_externo_norm",
    F.when(F.col("score_externo") > 0,
           F.col("score_externo") / 1000.0
    ).otherwise(F.lit(0.0))
).withColumn(
    # Flag binária de inadimplente (conservador: sem dado = assume inadimplente)
    "flag_inadimplente",
    F.when(F.col("num_restricoes").isNull(), F.lit(1))
     .when(F.col("num_restricoes") > 0, F.lit(1))
     .otherwise(F.lit(0))
).withColumn(
    # Score de relacionamento com o bureau (risco relativo)
    "score_risco_bureau",
    F.when(F.col("score_externo") >= 800, F.lit(1))   # Muito baixo risco
     .when(F.col("score_externo") >= 600, F.lit(2))   # Baixo risco
     .when(F.col("score_externo") >= 400, F.lit(3))   # Médio risco
     .when(F.col("score_externo") >= 200, F.lit(4))   # Alto risco
     .otherwise(F.lit(5))                              # Muito alto / sem info
)


# ── CONSOLIDAÇÃO DO VETOR DE FEATURES ────────────────────────
df_features = (df_clientes
    # Join com bureau (left: clientes sem consulta bureau recebem defaults)
    .join(df_bureau_features, "cpf_hash", "left")

    # Join com features transacionais via client_id
    .join(df_tx_features,
          df_clientes.client_id == df_tx_features.client_id, "left")

    # Metadados obrigatórios para o Feature Store
    .withColumn("feature_ts",    F.current_timestamp())
    .withColumn("run_date",      F.lit(run_date))
    .withColumn("feature_group", F.lit("credit-features-v1"))

    # Preenche nulos com defaults seguros de negócio
    .fillna({
        # Bureau: sem dado = situação desconhecida (conservador)
        "score_externo":       0.0,
        "score_externo_norm":  0.0,
        "num_restricoes":      0,
        "divida_total":        0.0,
        "flag_cheque":         0,
        "meses_negativado":    0,
        "flag_inadimplente":   1,    # Sem dado → assume inadimplente
        "score_risco_bureau":  5,    # Sem dado → maior risco

        # Transacional: sem histórico = cliente novo ou inativo
        "qtd_tx_30d":          0,
        "volume_tx_30d":       0.0,
        "ticket_medio_30d":    0.0,
        "ratio_credito_30d":   0.0,
        "qtd_tx_90d":          0,
        "volume_tx_90d":       0.0,
        "ticket_medio_90d":    0.0,
        "ratio_credito_90d":   0.0,
        "qtd_tx_180d":         0,
        "volume_tx_180d":      0.0,
    })

    # Feature derivada: utilização do limite (indicador chave de risco)
    .withColumn("utilizacao_limite",
        F.when(
            (F.col("limite_aprovado") > 0) & (F.col("volume_tx_30d") > 0),
            F.least(F.col("volume_tx_30d") / F.col("limite_aprovado"), F.lit(2.0))
        ).otherwise(F.lit(0.0)))

    # Feature derivada: tendência transacional (crescimento 30d vs 90d normalizado)
    .withColumn("tendencia_volume",
        F.when(
            (F.col("volume_tx_90d") > 0),
            (F.col("volume_tx_30d") * 3.0) / F.col("volume_tx_90d")
        ).otherwise(F.lit(1.0)))

    # Seleciona e ordena o vetor de features final
    .select(
        # Identificadores
        "cpf_hash", "client_id",

        # Features cadastrais
        "faixa_etaria", "faixa_renda_log", "renda_padronizada",
        "dias_relacionamento", "anos_relacionamento", "cliente_antigo",
        "status_cliente", "limite_aprovado", "qtd_contratos_ativos",

        # Features bureau
        "score_externo", "score_externo_norm", "score_risco_bureau",
        "num_restricoes", "divida_total", "flag_cheque",
        "meses_negativado", "flag_inadimplente",

        # Features transacionais 30d
        "qtd_tx_30d", "volume_tx_30d", "ticket_medio_30d",
        "maior_tx_30d", "ratio_credito_30d",

        # Features transacionais 90d
        "qtd_tx_90d", "volume_tx_90d", "ticket_medio_90d",
        "maior_tx_90d", "ratio_credito_90d",

        # Features transacionais 180d
        "qtd_tx_180d", "volume_tx_180d", "ticket_medio_180d",
        "ratio_credito_180d",

        # Features derivadas
        "utilizacao_limite", "tendencia_volume",

        # Metadados
        "feature_ts", "run_date", "feature_group",
    )
)

feature_count = df_features.count()
logger.info(f"Features calculadas para {feature_count:,} clientes "
            f"({df_features.columns.__len__()} features)")

# ── GRAVAÇÃO NO S3 GOLD ───────────────────────────────────────
year, month, day = run_date.split("-")
gold_path = (f"s3://{GOLD_BUCKET}/credit-features/"
             f"year={year}/month={month}/day={day}/")

(df_features.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(gold_path))

logger.info(f"✅ Features salvas em {gold_path}")
logger.info(f"   Total: {feature_count:,} clientes | "
            f"{len(df_features.columns)} features por cliente")

job.commit()
