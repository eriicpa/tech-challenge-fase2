"""AWS Glue Job — Camada BRONZE da pipeline de alfabetização.

Ingere as fontes do desafio e grava a Bronze em Parquet particionado, preservando
o dado bruto e anexando linhagem. Espelho do Notebook 01, Etapa 2.

Parâmetros do job (Job details -> Advanced properties -> Job parameters):
    --BUCKET_LANDING   420411424817-tc2-landing
    --BUCKET_SOR       420411424817-tc2-data-sor
    --ENTIDADES        indicador_municipio,indicador_uf,meta_brasil,meta_uf,meta_municipio
    --API_IBGE         https://servicodados.ibge.gov.br/api/v1/localidades

Recomendação de execução (FinOps):
    Worker type G.1X | Number of workers 2 | Job bookmark: enable | Auto scaling: on
"""
import sys
import logging
from datetime import datetime, timezone

import requests
import pandas as pd

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ============================================================
# PARÂMETROS
# ============================================================
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "BUCKET_LANDING", "BUCKET_SOR", "ENTIDADES", "API_IBGE",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.sparkContext.setLogLevel("WARN")

JOB_NAME       = args["JOB_NAME"]
BUCKET_LANDING = args["BUCKET_LANDING"]
BUCKET_SOR     = args["BUCKET_SOR"]
ENTIDADES      = [e.strip() for e in args["ENTIDADES"].split(",") if e.strip()]
API_IBGE       = args["API_IBGE"]

INGESTION_TS   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
ANO, MES, DIA  = INGESTION_DATE.split("-")

ARQUIVOS = {
    "indicador_municipio": "br_inep_avaliacao_alfabetizacao_municipio.csv",
    "indicador_uf":        "br_inep_avaliacao_alfabetizacao_uf.csv",
    "meta_brasil":         "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv",
    "meta_uf":             "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv",
    "meta_municipio":      "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv",
}

# Regras mínimas de aceitação por entidade — o job falha (e alerta) se não passarem.
CHECKS = {
    "indicador_municipio": [("min_count", None, 10000), ("not_null", "id_municipio", None),
                            ("not_null", "taxa_alfabetizacao", None)],
    "indicador_uf":        [("min_count", None, 100), ("not_null", "sigla_uf", None)],
    "meta_brasil":         [("min_count", None, 1), ("not_null", "ano", None)],
    "meta_uf":             [("min_count", None, 20), ("not_null", "sigla_uf", None)],
    "meta_municipio":      [("min_count", None, 10000), ("not_null", "id_municipio", None)],
    "dim_uf_ibge":         [("min_count", None, 27), ("unique", "id", None)],
    "dim_municipio_ibge":  [("min_count", None, 5500), ("unique", "id", None)],
}

log.info("=" * 70)
log.info("JOB      : %s", JOB_NAME)
log.info("ENTIDADES: %s", ENTIDADES)
log.info("DESTINO  : s3://%s/bronze/", BUCKET_SOR)
log.info("=" * 70)


# ============================================================
# FUNÇÕES
# ============================================================
def com_linhagem(df, entidade, origem, formato):
    """Anexa as colunas de linhagem e as partições físicas de ingestão."""
    return (df
            .withColumn("_ingestion_timestamp", F.lit(INGESTION_TS))
            .withColumn("_ingestion_date", F.lit(INGESTION_DATE))
            .withColumn("_source", F.lit(origem))
            .withColumn("_source_format", F.lit(formato))
            .withColumn("_entity", F.lit(entidade))
            .withColumn("_pipeline_run_id", F.lit(JOB_NAME + "_" + INGESTION_TS))
            .withColumn("_record_hash", F.sha2(F.concat_ws("||", *df.columns), 256))
            .withColumn("ano_ingestao", F.lit(ANO))
            .withColumn("mes_ingestao", F.lit(MES))
            .withColumn("dia_ingestao", F.lit(DIA)))


def checar_qualidade(df, entidade):
    """Quality gate da Bronze. Falha crítica interrompe o job."""
    regras = CHECKS.get(entidade, [])
    if not regras:
        log.warning("[DQ:BRONZE] Sem regras para '%s' — pulando", entidade)
        return

    falhas = []
    for tipo, coluna, valor in regras:
        if tipo == "min_count":
            total = df.count()
            ok, detalhe = total >= valor, f"contagem={total} minimo={valor}"
        elif tipo == "not_null":
            nulos = df.filter(F.col(coluna).isNull()).count()
            ok, detalhe = nulos == 0, f"{nulos} nulos em {coluna}"
        elif tipo == "unique":
            duplicadas = df.count() - df.select(coluna).distinct().count()
            ok, detalhe = duplicadas == 0, f"{duplicadas} duplicatas em {coluna}"
        else:
            ok, detalhe = True, "tipo de check desconhecido"

        log.info("[DQ:BRONZE] %-4s | %-10s | %s", "PASS" if ok else "FAIL", tipo, detalhe)
        if not ok:
            falhas.append(f"{tipo}:{coluna}:{detalhe}")

    if falhas:
        raise Exception(f"[QUALITY GATE] {entidade}: " + "; ".join(falhas))


def gravar(df, entidade):
    destino = f"s3://{BUCKET_SOR}/bronze/{entidade}"
    (df.write
       .mode("overwrite")
       .option("compression", "snappy")
       .partitionBy("ano_ingestao", "mes_ingestao", "dia_ingestao")
       .parquet(destino))
    log.info("[BRONZE] %s | %d linhas -> %s", entidade, df.count(), destino)
    return destino


# ============================================================
# EXECUÇÃO — CSVs DA LANDING
# ============================================================
for entidade in ENTIDADES:
    arquivo = ARQUIVOS.get(entidade)
    if not arquivo:
        log.warning("Entidade desconhecida, ignorando: %s", entidade)
        continue

    origem = f"s3://{BUCKET_LANDING}/basedosdados/{arquivo}"
    log.info("[INGESTAO] Lendo %s", origem)
    bruto = (spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv(origem))

    bronze = com_linhagem(bruto, entidade, origem, "csv")
    checar_qualidade(bronze, entidade)
    gravar(bronze, entidade)

# ============================================================
# EXECUÇÃO — API DE LOCALIDADES DO IBGE
# ============================================================
for entidade, rota in (("dim_uf_ibge", "estados"), ("dim_municipio_ibge", "municipios")):
    url = f"{API_IBGE}/{rota}"
    log.info("[INGESTAO] GET %s", url)
    resposta = requests.get(url, timeout=60)
    resposta.raise_for_status()

    achatado = pd.json_normalize(resposta.json())
    achatado.columns = [c.replace(".", "_").replace("-", "_") for c in achatado.columns]
    bronze = com_linhagem(spark.createDataFrame(achatado), entidade, url, "json")
    checar_qualidade(bronze, entidade)
    gravar(bronze, entidade)

log.info("=" * 70)
log.info("BRONZE CONCLUIDA | proxima etapa: etl_silver")
log.info("=" * 70)

job.commit()