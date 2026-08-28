"""AWS Glue Job — Camada SILVER da pipeline de alfabetização.

Padroniza tipos, decodifica domínios, normaliza chaves, deduplica pelo grão e
**integra** indicador + metas + território. Espelho do Notebook 01, Etapa 4.

Parâmetros do job:
    --BUCKET_SOR   420411424817-tc2-data-sor
    --BUCKET_SOT   420411424817-tc2-data-sot
"""
import sys
import logging
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)-8s | %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%SZ")
log = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_SOR", "BUCKET_SOT"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "16")
spark.sparkContext.setLogLevel("WARN")

BUCKET_SOR = args["BUCKET_SOR"]
BUCKET_SOT = args["BUCKET_SOT"]
RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

PONTO_CORTE_SAEB = 743
META_NACIONAL_2030 = 80.0
DIM_REDE = {0: "Total", 1: "Federal", 2: "Estadual", 3: "Municipal", 4: "Privada", 5: "Publica"}
REDE_TEXTO = {"municipal": 3, "publica": 5, "pública": 5, "estadual": 2, "federal": 1, "privada": 4}


def ler_bronze(entidade):
    return spark.read.parquet(f"s3://{BUCKET_SOR}/bronze/{entidade}")


def gravar_silver(df, tabela, particoes=None):
    destino = f"s3://{BUCKET_SOT}/silver/{tabela}"
    escritor = (df.withColumn("_silver_timestamp", F.lit(RUN_TS))
                  .write.mode("overwrite").option("compression", "snappy"))
    if particoes:
        escritor = escritor.partitionBy(*particoes)
    escritor.parquet(destino)
    log.info("[SILVER] %-32s | %8d linhas -> %s", tabela, df.count(), destino)
    return destino


def mapear_rede(coluna):
    """Traduz o código numérico da rede para nome, via CASE WHEN encadeado."""
    expressao = F.lit(None).cast("string")
    for codigo, nome in DIM_REDE.items():
        expressao = F.when(coluna == codigo, F.lit(nome)).otherwise(expressao)
    return expressao


# ============================================================
# DIMENSÕES TERRITORIAIS
# ============================================================
dim_uf = (ler_bronze("dim_uf_ibge")
          .select(F.col("id").cast("int").alias("id_uf"),
                  F.upper(F.trim(F.col("sigla"))).alias("sigla_uf"),
                  F.trim(F.col("nome")).alias("nome_uf"),
                  F.col("regiao_sigla").alias("sigla_regiao"),
                  F.col("regiao_nome").alias("nome_regiao"))
          .dropDuplicates(["id_uf"]))
gravar_silver(dim_uf, "dim_uf")

dim_municipio = (ler_bronze("dim_municipio_ibge")
                 .select(F.col("id").cast("int").alias("id_municipio"),
                         F.trim(F.col("nome")).alias("nome_municipio"),
                         F.col("microrregiao_nome").alias("nome_microrregiao"),
                         F.col("microrregiao_mesorregiao_nome").alias("nome_mesorregiao"))
                 .withColumn("id_uf", (F.col("id_municipio") / 100000).cast("int"))
                 .join(broadcast(dim_uf), on="id_uf", how="left")
                 .filter(F.col("sigla_uf").isNotNull()))
gravar_silver(dim_municipio, "dim_municipio")

# ============================================================
# FATOS DO INDICADOR
# ============================================================
COLUNAS_NIVEL = [f"proporcao_aluno_nivel_{i}" for i in range(9)]

indicador_municipio = (ler_bronze("indicador_municipio")
    .withColumn("ano", F.col("ano").cast("int"))
    .withColumn("id_municipio", F.col("id_municipio").cast("int"))
    .withColumn("serie", F.col("serie").cast("int"))
    .withColumn("rede", F.col("rede").cast("int"))
    .withColumn("taxa_alfabetizacao", F.round(F.col("taxa_alfabetizacao").cast("double"), 2))
    .withColumn("media_portugues", F.round(F.col("media_portugues").cast("double"), 2))
    .filter(F.col("ano").isNotNull()
            & F.col("id_municipio").between(1000000, 9999999)
            & F.col("taxa_alfabetizacao").between(0, 100))
    .withColumn("rede_nome", mapear_rede(F.col("rede")))
    .dropDuplicates(["ano", "id_municipio", "serie", "rede"]))

fato_indicador_municipio = (indicador_municipio
    .select("ano", "id_municipio", "serie", "rede", "rede_nome",
            "taxa_alfabetizacao", "media_portugues")
    .withColumn("id_uf", (F.col("id_municipio") / 100000).cast("int"))
    .withColumn("alcancou_meta_2030", F.col("taxa_alfabetizacao") >= F.lit(META_NACIONAL_2030)))
gravar_silver(fato_indicador_municipio, "fato_indicador_municipio", ["ano"])

fato_indicador_uf = (ler_bronze("indicador_uf")
    .withColumn("ano", F.col("ano").cast("int"))
    .withColumn("rede", F.col("rede").cast("int"))
    .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
    .withColumn("taxa_alfabetizacao", F.round(F.col("taxa_alfabetizacao").cast("double"), 2))
    .withColumn("rede_nome", mapear_rede(F.col("rede")))
    .dropDuplicates(["ano", "sigla_uf", "serie", "rede"])
    .join(broadcast(dim_uf.select("sigla_uf", "id_uf", "nome_uf", "nome_regiao")),
          on="sigla_uf", how="left"))
gravar_silver(fato_indicador_uf, "fato_indicador_uf", ["ano"])

# Normalização: 9 colunas de proporção viram uma tabela longa
distribuicao = None
for nivel, coluna in enumerate(COLUNAS_NIVEL):
    parcial = (indicador_municipio
               .select("ano", "id_municipio", "rede", "rede_nome",
                       F.col(coluna).cast("double").alias("proporcao_alunos"))
               .withColumn("nivel", F.lit(nivel))
               .filter(F.col("proporcao_alunos").isNotNull()))
    distribuicao = parcial if distribuicao is None else distribuicao.unionByName(parcial)
gravar_silver(distribuicao, "fato_distribuicao_nivel", ["ano"])


# ============================================================
# TRAJETÓRIAS DE META (colunas por ano -> linhas)
# ============================================================
def trajetoria_metas(df, chaves):
    colunas_meta = [c for c in df.columns if c.startswith("meta_alfabetizacao_")]
    empilhados = None
    for coluna in colunas_meta:
        ano_meta = int(coluna.split("_")[-1])
        parcial = (df.select(*chaves, "rede", "ano",
                             F.col(coluna).cast("double").alias("meta_taxa"))
                     .withColumn("ano_meta", F.lit(ano_meta))
                     .filter(F.col("meta_taxa").isNotNull()))
        empilhados = parcial if empilhados is None else empilhados.unionByName(parcial)

    # Revisão vigente: a declarada no ano de referência mais recente
    janela = Window.partitionBy(*chaves, "rede", "ano_meta").orderBy(F.col("ano").desc())
    return (empilhados
            .withColumn("_ordem", F.row_number().over(janela))
            .filter(F.col("_ordem") == 1)
            .drop("_ordem", "ano"))


def normalizar_rede_texto(df):
    expressao = F.lit(None).cast("int")
    for texto, codigo in REDE_TEXTO.items():
        expressao = F.when(F.lower(F.trim(F.col("rede"))) == texto,
                           F.lit(codigo)).otherwise(expressao)
    return df.withColumn("rede", expressao)


meta_municipio = normalizar_rede_texto(
    ler_bronze("meta_municipio").withColumn("id_municipio", F.col("id_municipio").cast("int")))
gravar_silver(trajetoria_metas(meta_municipio, ["id_municipio"]), "dim_meta_municipio")

meta_uf = normalizar_rede_texto(
    ler_bronze("meta_uf").withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf")))))
gravar_silver(trajetoria_metas(meta_uf, ["sigla_uf"]), "dim_meta_uf")

# ============================================================
# TABELA INTEGRADA
# ============================================================
metas_municipio = spark.read.parquet(f"s3://{BUCKET_SOT}/silver/dim_meta_municipio")
meta_2030 = (metas_municipio.filter(F.col("ano_meta") == 2030)
             .select("id_municipio", "rede", F.col("meta_taxa").alias("meta_taxa_2030")))
contexto_uf = (fato_indicador_uf
               .select("ano", "sigla_uf", "rede", F.col("taxa_alfabetizacao").alias("taxa_uf")))

integrada = (fato_indicador_municipio
    .join(broadcast(dim_municipio.select("id_municipio", "nome_municipio", "sigla_uf",
                                         "nome_uf", "nome_regiao", "nome_mesorregiao")),
          on="id_municipio", how="left")
    .join(metas_municipio.select("id_municipio", "rede",
                                 F.col("ano_meta").alias("ano"),
                                 F.col("meta_taxa").alias("meta_taxa_ano")),
          on=["id_municipio", "rede", "ano"], how="left")
    .join(broadcast(meta_2030), on=["id_municipio", "rede"], how="left")
    .join(contexto_uf, on=["ano", "sigla_uf", "rede"], how="left")
    .withColumn("meta_taxa_2030", F.coalesce(F.col("meta_taxa_2030"), F.lit(META_NACIONAL_2030)))
    .withColumn("gap_meta_ano", F.round(F.col("taxa_alfabetizacao") - F.col("meta_taxa_ano"), 2))
    .withColumn("atingiu_meta_ano", F.col("gap_meta_ano") >= 0)
    .withColumn("gap_meta_2030", F.round(F.col("meta_taxa_2030") - F.col("taxa_alfabetizacao"), 2))
    .withColumn("dif_vs_uf", F.round(F.col("taxa_alfabetizacao") - F.col("taxa_uf"), 2)))

# ── Quality gate da Silver ──
total = integrada.count()
duplicadas = total - integrada.dropDuplicates(["ano", "id_municipio", "rede"]).count()
sem_territorio = integrada.filter(F.col("sigla_uf").isNull()).count()
log.info("[DQ:SILVER] linhas=%d duplicadas=%d sem_territorio=%d", total, duplicadas, sem_territorio)
if duplicadas > 0 or sem_territorio > 0:
    raise Exception(f"[QUALITY GATE] Silver integrada: {duplicadas} duplicatas, "
                    f"{sem_territorio} linhas sem território")

gravar_silver(integrada, "fato_alfabetizacao_municipio", ["ano"])

log.info("=" * 70)
log.info("SILVER CONCLUIDA | proxima etapa: etl_gold")
log.info("=" * 70)

job.commit()