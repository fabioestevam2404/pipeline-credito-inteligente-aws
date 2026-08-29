"""
src/processing/glue_silver_processing.py
Glue ETL Job — Processamento Silver (limpeza, validação, deduplicação, Iceberg UPSERT).

Fluxo:
  S3 Raw (Parquet) → Validação → Transformações → S3 Silver (Iceberg MERGE)

Depende de:
  - Tabela Iceberg credit_db.clientes_silver já criada (ver create_iceberg_tables.sql)
  - Glue version 4.0 com suporte a Iceberg
"""
import logging
import sys
from datetime import datetime

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ── Inicialização ─────────────────────────────────────────────
args     = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_BUCKET", "SILVER_BUCKET"])
run_date = next((a.split("=", 1)[1] for a in sys.argv if a.startswith("--run_date=")),
                datetime.utcnow().strftime("%Y-%m-%d"))

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_BUCKET    = args["RAW_BUCKET"]
SILVER_BUCKET = args["SILVER_BUCKET"]

# ── Configuração Iceberg ──────────────────────────────────────
spark.conf.set(
    "spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
)
spark.conf.set("spark.sql.catalog.glue_catalog",   "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{SILVER_BUCKET}/")
spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")

# ── Leitura do Raw ────────────────────────────────────────────
year, month, day = run_date.split("-")
raw_path = f"s3://{RAW_BUCKET}/crm/clientes/partition_year={year}/partition_month={month}/partition_day={day}/"

logger.info(f"Lendo dados raw de: {raw_path}")

try:
    df_raw = spark.read.parquet(raw_path)
except Exception as e:
    logger.error(f"Erro ao ler S3 Raw: {e}")
    raise

total_raw = df_raw.count()
logger.info(f"Total raw: {total_raw:,} registros")


# ── VALIDAÇÃO DE QUALIDADE ────────────────────────────────────
def validate_and_report(df: DataFrame, total: int) -> tuple[DataFrame, dict]:
    """
    Aplica regras de validação e retorna DataFrame limpo + relatório.
    Registros inválidos são descartados com log do motivo.
    """
    metrics = {"total_input": total, "dropped": {}}

    # Regra 1: CPF hash obrigatório e com tamanho correto (SHA-256 = 64 chars)
    mask_cpf = F.col("cpf_hash").isNotNull() & (F.length("cpf_hash") == 64)
    invalid_cpf = df.filter(~mask_cpf).count()
    df = df.filter(mask_cpf)
    if invalid_cpf:
        logger.warning(f"[DQ] {invalid_cpf} registros descartados: CPF hash inválido")
        metrics["dropped"]["cpf_invalido"] = invalid_cpf

    # Regra 2: Renda declarada >= 0 ou nula
    mask_renda = F.col("renda_declarada").isNull() | (F.col("renda_declarada") >= 0)
    invalid_renda = df.filter(~mask_renda).count()
    df = df.filter(mask_renda)
    if invalid_renda:
        logger.warning(f"[DQ] {invalid_renda} registros descartados: renda inválida")
        metrics["dropped"]["renda_invalida"] = invalid_renda

    # Regra 3: Data de nascimento realista
    mask_nasc = (
        F.col("data_nascimento").isNull() |
        F.col("data_nascimento").between("1920-01-01", "2007-12-31")
    )
    invalid_nasc = df.filter(~mask_nasc).count()
    df = df.filter(mask_nasc)
    if invalid_nasc:
        logger.warning(f"[DQ] {invalid_nasc} registros descartados: data de nascimento inválida")
        metrics["dropped"]["nascimento_invalido"] = invalid_nasc

    # Regra 4: Status do cliente válido
    valid_statuses = ["ATIVO", "INATIVO", "BLOQUEADO", "ENCERRADO"]
    mask_status = F.col("status_cliente").isin(valid_statuses)
    invalid_status = df.filter(~mask_status).count()
    df = df.filter(mask_status)
    if invalid_status:
        logger.warning(f"[DQ] {invalid_status} registros descartados: status inválido")
        metrics["dropped"]["status_invalido"] = invalid_status

    total_dropped = sum(metrics["dropped"].values())
    drop_rate = total_dropped / max(total, 1) * 100
    metrics["total_valid"] = total - total_dropped
    metrics["drop_rate_pct"] = round(drop_rate, 2)

    logger.info(f"[DQ] Validação: {metrics['total_valid']:,} válidos, "
                f"{total_dropped:,} descartados ({drop_rate:.1f}%)")

    # Alerta se drop rate acima de 10%
    if drop_rate > 10:
        logger.warning(f"⚠️ Drop rate elevado: {drop_rate:.1f}% — investigar fonte")

    return df, metrics


df_valid, quality_metrics = validate_and_report(df_raw, total_raw)


# ── TRANSFORMAÇÕES ────────────────────────────────────────────
def apply_transformations(df: DataFrame, run_date: str) -> DataFrame:
    """
    Aplica transformações de negócio:
    - Normalização de strings
    - Cálculo de features derivadas
    - Padronização de tipos
    """
    return (df
        # Strings: trim + uppercase
        .withColumn("nome_normalizado",
            F.upper(F.trim(F.col("nome_completo"))))

        # Renda: nulos → sentinel -1 (indica "não informado")
        .withColumn("renda_padronizada",
            F.coalesce(F.col("renda_declarada").cast(DoubleType()), F.lit(-1.0)))

        # Faixa de renda em escala log (feature para o modelo)
        .withColumn("faixa_renda_log",
            F.when(F.col("renda_padronizada") <= 0,    F.lit(0))
             .when(F.col("renda_padronizada") <= 1500,  F.lit(1))
             .when(F.col("renda_padronizada") <= 3000,  F.lit(2))
             .when(F.col("renda_padronizada") <= 7000,  F.lit(3))
             .when(F.col("renda_padronizada") <= 15000, F.lit(4))
             .otherwise(F.lit(5)))

        # Faixa etária
        .withColumn("idade_anos",
            F.floor(F.months_between(F.current_date(), "data_nascimento") / 12).cast("int"))
        .withColumn("faixa_etaria",
            F.when(F.col("idade_anos") < 25, F.lit("18-24"))
             .when(F.col("idade_anos") < 35, F.lit("25-34"))
             .when(F.col("idade_anos") < 50, F.lit("35-49"))
             .when(F.col("idade_anos") < 65, F.lit("50-64"))
             .otherwise(F.lit("65+")))

        # Tempo de relacionamento
        .withColumn("dias_relacionamento",
            F.datediff(F.current_date(), "data_abertura_conta"))
        .withColumn("anos_relacionamento",
            F.round(F.col("dias_relacionamento") / 365.0, 1))

        # Flag de cliente de longo prazo (> 2 anos)
        .withColumn("cliente_antigo",
            (F.col("dias_relacionamento") > 730).cast("int"))

        # Metadados de processamento
        .withColumn("processing_date",    F.lit(run_date))
        .withColumn("processing_ts",      F.current_timestamp())
        .withColumn("pipeline_version",   F.lit("1.0.0"))

        # Seleciona apenas colunas necessárias (não propaga colunas raw desnecessárias)
        .select(
            "client_id", "cpf_hash", "nome_normalizado",
            "data_nascimento", "idade_anos", "faixa_etaria",
            "cidade", "estado",
            "renda_padronizada", "faixa_renda_log",
            "data_abertura_conta", "dias_relacionamento",
            "anos_relacionamento", "cliente_antigo",
            "status_cliente", "limite_aprovado",
            "qtd_contratos_ativos",
            "processing_date", "processing_ts", "pipeline_version",
            "updated_at",
        )
    )


df_transformed = apply_transformations(df_valid, run_date)


# ── DEDUPLICAÇÃO ──────────────────────────────────────────────
# Mantém apenas o registro mais recente por CPF
window_cpf = Window.partitionBy("cpf_hash").orderBy(F.desc("updated_at"))
df_dedup = (df_transformed
    .withColumn("_row_num", F.row_number().over(window_cpf))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num"))

final_count = df_dedup.count()
logger.info(f"Após deduplicação: {final_count:,} registros únicos")


# ── UPSERT ICEBERG ────────────────────────────────────────────
# Cria tabela Silver se não existir
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.credit_db.clientes_silver (
        client_id            STRING,
        cpf_hash             STRING,
        nome_normalizado     STRING,
        data_nascimento      DATE,
        idade_anos           INT,
        faixa_etaria         STRING,
        cidade               STRING,
        estado               STRING,
        renda_padronizada    DOUBLE,
        faixa_renda_log      INT,
        data_abertura_conta  DATE,
        dias_relacionamento  INT,
        anos_relacionamento  DOUBLE,
        cliente_antigo       INT,
        status_cliente       STRING,
        limite_aprovado      DOUBLE,
        qtd_contratos_ativos INT,
        processing_date      STRING,
        processing_ts        TIMESTAMP,
        pipeline_version     STRING,
        updated_at           TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (status_cliente, processing_date)
    LOCATION 's3://{SILVER_BUCKET}/clientes_silver/'
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'history.expire.max-snapshot-age-ms' = '604800000'
    )
""")

# Registra DataFrame como view temporária para o MERGE
df_dedup.createOrReplaceTempView("silver_updates")

# MERGE INTO: atualiza se existe, insere se não existe
spark.sql("""
    MERGE INTO glue_catalog.credit_db.clientes_silver AS target
    USING silver_updates AS source
    ON target.cpf_hash = source.cpf_hash
    WHEN MATCHED AND source.updated_at > target.updated_at THEN
        UPDATE SET
            nome_normalizado     = source.nome_normalizado,
            idade_anos           = source.idade_anos,
            faixa_etaria         = source.faixa_etaria,
            renda_padronizada    = source.renda_padronizada,
            faixa_renda_log      = source.faixa_renda_log,
            dias_relacionamento  = source.dias_relacionamento,
            anos_relacionamento  = source.anos_relacionamento,
            cliente_antigo       = source.cliente_antigo,
            status_cliente       = source.status_cliente,
            limite_aprovado      = source.limite_aprovado,
            qtd_contratos_ativos = source.qtd_contratos_ativos,
            processing_date      = source.processing_date,
            processing_ts        = source.processing_ts,
            updated_at           = source.updated_at
    WHEN NOT MATCHED THEN
        INSERT *
""")

logger.info(f"✅ Silver processing concluído: {final_count:,} registros processados")

# ── Publicação de Métricas ────────────────────────────────────
cw = boto3.client("cloudwatch")
total_dropped = sum(quality_metrics["dropped"].values())
drop_rate = quality_metrics["drop_rate_pct"]

cw.put_metric_data(
    Namespace  = "CreditPipeline/Custom",
    MetricData = [
        {"MetricName": "RecordsProcessed", "Value": float(final_count),   "Unit": "Count"},
        {"MetricName": "RecordsDropped",   "Value": float(total_dropped),  "Unit": "Count"},
        {"MetricName": "DataQualityRate",  "Value": 100.0 - drop_rate,     "Unit": "Percent"},
        {"MetricName": "RecordDropRate",   "Value": drop_rate,             "Unit": "Percent"},
    ]
)

job.commit()
