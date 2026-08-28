"""AWS Glue Job — Camada GOLD da pipeline de alfabetização.

Executa as MESMAS consultas SQL do Notebook 03 (`src/gold/consultas.py`), lendo a
Silver do bucket SOT e gravando a Gold no bucket SPEC.

Parâmetros do job:
    --BUCKET_SOT    420411424817-tc2-data-sot
    --BUCKET_SPEC   420411424817-tc2-data-spec
    --TABELAS       todas            (ou lista separada por vírgula)

Dependência:
    Job details -> Advanced properties -> Python library path:
        s3://<bucket-artefatos>/libs/consultas.py

    Esse arquivo é o próprio `src/gold/consultas.py` versionado no repositório —
    a regra de negócio da Gold tem uma única fonte da verdade.
"""
import sys
import json
import logging
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

from consultas import CONSULTAS_GOLD

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)-8s | %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%SZ")
log = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_SOT", "BUCKET_SPEC", "TABELAS"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "16")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.sparkContext.setLogLevel("WARN")

BUCKET_SOT  = args["BUCKET_SOT"]
BUCKET_SPEC = args["BUCKET_SPEC"]
SELECAO     = args["TABELAS"]
RUN_TS      = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

VIEWS_SILVER = {
    "silver_alfabetizacao_municipio": "fato_alfabetizacao_municipio",
    "silver_indicador_uf":            "fato_indicador_uf",
    "silver_dim_municipio":           "dim_municipio",
    "silver_dim_uf":                  "dim_uf",
    "silver_meta_municipio":          "dim_meta_municipio",
    "silver_meta_uf":                 "dim_meta_uf",
    "silver_distribuicao_nivel":      "fato_distribuicao_nivel",
    "silver_aluno":                   "fato_aluno",
    "silver_avaliacao_stream":        "fato_avaliacao_stream",
}

# ============================================================
# REGISTRO DAS VIEWS
# ============================================================
registradas = set()
for view, tabela in VIEWS_SILVER.items():
    caminho = f"s3://{BUCKET_SOT}/silver/{tabela}"
    try:
        spark.read.parquet(caminho).createOrReplaceTempView(view)
        registradas.add(view)
        log.info("[GOLD] View registrada: %s", view)
    except Exception as exc:
        log.warning("[GOLD] Tabela Silver ausente (%s): %s", tabela, exc)

# ============================================================
# EXECUÇÃO DAS CONSULTAS
# ============================================================
alvos = (list(CONSULTAS_GOLD) if SELECAO.strip().lower() == "todas"
         else [t.strip() for t in SELECAO.split(",") if t.strip()])

metricas = []
for nome in alvos:
    spec = CONSULTAS_GOLD[nome]

    if "silver_avaliacao_stream" in spec["sql"] and "silver_avaliacao_stream" not in registradas:
        log.warning("[GOLD] %s ignorada: depende do stream, ainda não disponível", nome)
        continue

    inicio = datetime.now(timezone.utc)
    try:
        resultado = spark.sql(spec["sql"])
        destino = f"s3://{BUCKET_SPEC}/gold/{nome}"
        escritor = resultado.write.mode("overwrite").option("compression", "snappy")
        if spec.get("particoes"):
            escritor = escritor.partitionBy(*spec["particoes"])
        escritor.parquet(destino)

        linhas = resultado.count()
        duracao = (datetime.now(timezone.utc) - inicio).total_seconds()
        metricas.append({"tabela": nome, "linhas": linhas, "segundos": round(duracao, 2),
                         "destino": destino, "status": "OK"})
        log.info("[GOLD] %-30s | %8d linhas | %6.2fs -> %s", nome, linhas, duracao, destino)
    except Exception as exc:
        if spec.get("opcional"):
            log.warning("[GOLD] %s falhou e é opcional: %s", nome, exc)
            metricas.append({"tabela": nome, "linhas": 0, "segundos": 0,
                             "destino": "-", "status": f"IGNORADA: {exc}"})
        else:
            log.error("[GOLD] %s falhou: %s", nome, exc)
            raise

log.info("=" * 70)
log.info("GOLD CONCLUIDA | %s", json.dumps(metricas, ensure_ascii=False))
log.info("Proxima etapa: Glue Crawler -> Athena (ver cloud/aws/athena/ddl_gold.sql)")
log.info("=" * 70)

job.commit()