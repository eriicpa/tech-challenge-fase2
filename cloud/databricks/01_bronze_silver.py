# Databricks notebook source
# MAGIC %md
# MAGIC # Tech Challenge - Fase 2 | Databricks
# MAGIC ## Notebook 01 - Bronze e Silver com Delta Lake e Unity Catalog
# MAGIC
# MAGIC Implementação da pipeline no Databricks Free Edition, seguindo a arquitetura Lakehouse
# MAGIC descrita na Aula 03: formatos abertos, metadados, garantias ACID e esquema medalhão sobre uma
# MAGIC única fonte de dados.
# MAGIC
# MAGIC | Ambiente local | Databricks Free Edition |
# MAGIC |---|---|
# MAGIC | Parquet em pastas (`data/lake/bronze/...`) | tabelas Delta no Unity Catalog (`tc2_bronze.*`) |
# MAGIC | Ingestão da API do IBGE por HTTP | CSV no volume, porque a saída para a internet é restrita |
# MAGIC | Particionamento físico por data de ingestão | coluna comum e `OPTIMIZE`/`ZORDER` do Delta |
# MAGIC | Dedup por `drop_duplicates` | `MERGE INTO` transacional |
# MAGIC
# MAGIC A terceira linha é uma decisão, não uma limitação. Particionar fisicamente uma tabela de
# MAGIC 24 mil linhas cria arquivos pequenos demais e piora a leitura. No S3 o particionamento se paga
# MAGIC porque o Athena cobra por byte escaneado. No Delta, o data skipping por estatísticas de
# MAGIC arquivo já resolve, e o particionamento vira custo.
# MAGIC
# MAGIC ### Pré-requisitos
# MAGIC
# MAGIC 1. Volume criado e os 8 arquivos enviados: 7 CSVs e o Parquet de microdados de aluno
# MAGIC    (a primeira célula cria o volume; a segunda confere o que falta).
# MAGIC 2. Repositório importado como Git folder, para que `src/` fique acessível.
# MAGIC
# MAGIC O passo a passo completo está em `docs/guia_deploy_databricks.md`.

# COMMAND ----------

# ============================================================
# PARÂMETROS DO JOB
# ============================================================
# Equivalente ao "Job parameters" do AWS Glue (--BUCKET_SOR, --ENTIDADE...).
# No Glue são lidos com getResolvedOptions(sys.argv, [...]); aqui, com widgets.
# Definidos assim, aparecem como campos editáveis no topo do notebook e podem ser
# sobrescritos pela tarefa do Lakeflow sem tocar no código.
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
dbutils.widgets.text("schema_landing", "tc2_landing", "Schema de landing")
dbutils.widgets.text("schema_bronze", "tc2_bronze", "Schema Bronze")
dbutils.widgets.text("schema_silver", "tc2_silver", "Schema Silver")
dbutils.widgets.text("volume", "arquivos", "Volume dos arquivos")

CATALOGO = dbutils.widgets.get("catalogo")
SCHEMA_LANDING = dbutils.widgets.get("schema_landing")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
SCHEMA_SILVER = dbutils.widgets.get("schema_silver")
VOLUME_NOME = dbutils.widgets.get("volume")

RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

PONTO_CORTE_SAEB = 743
META_NACIONAL_2030 = 80.0
DIM_REDE = {0: "Total", 1: "Federal", 2: "Estadual", 3: "Municipal", 4: "Privada", 5: "Publica"}
REDE_TEXTO = {"municipal": 3, "publica": 5, "pública": 5, "estadual": 2, "federal": 1, "privada": 4}

for schema in (SCHEMA_LANDING, SCHEMA_BRONZE, SCHEMA_SILVER):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA_LANDING}.{VOLUME_NOME}")

VOLUME = f"/Volumes/{CATALOGO}/{SCHEMA_LANDING}/{VOLUME_NOME}"
print(f"Volume de landing: {VOLUME}")
print(f"Run: {RUN_TS}")

# COMMAND ----------

# ============================================================
# CONFERÊNCIA DOS ARQUIVOS ENVIADOS
# ============================================================
ARQUIVOS = {
    "indicador_municipio": "br_inep_avaliacao_alfabetizacao_municipio.csv",
    "indicador_uf":        "br_inep_avaliacao_alfabetizacao_uf.csv",
    "meta_brasil":         "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv",
    "meta_uf":             "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv",
    "meta_municipio":      "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv",
    "dim_uf_ibge":         "ibge_estados.csv",
    "dim_municipio_ibge":  "ibge_municipios.csv",
}

# Microdados de aluno: 3,87 milhões de linhas, em Parquet
ARQUIVO_ALUNOS = "alunos.parquet"

presentes = {f.name for f in dbutils.fs.ls(VOLUME)}
faltando = [a for a in list(ARQUIVOS.values()) + [ARQUIVO_ALUNOS] if a not in presentes]

if faltando:
    raise Exception(
        f"Faltam {len(faltando)} arquivo(s) no volume {VOLUME}:\n  - "
        + "\n  - ".join(faltando)
        + f"\n\nEnvie-os pela UI: Catalog > {SCHEMA_LANDING} > {VOLUME_NOME} > "
          "Upload to this volume."
    )

print(f"Os {len(ARQUIVOS) + 1} arquivos estão no volume.")
display(dbutils.fs.ls(VOLUME))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Camada Bronze
# MAGIC
# MAGIC Dados como vieram da fonte, com colunas de linhagem. A diferença para a versão local é o
# MAGIC destino: em vez de Parquet numa pasta, tabelas Delta gerenciadas pelo Unity Catalog. Isso traz
# MAGIC controle de acesso, histórico de versões (time travel) e `DESCRIBE HISTORY` para auditoria.

# COMMAND ----------

# ============================================================
# INGESTÃO PARA A BRONZE
# ============================================================
def com_linhagem(df, entidade, origem):
    return (df
            .withColumn("_ingestion_timestamp", F.lit(RUN_TS))
            .withColumn("_ingestion_date", F.lit(INGESTION_DATE))
            .withColumn("_source", F.lit(origem))
            .withColumn("_source_format", F.lit("csv"))
            .withColumn("_entity", F.lit(entidade))
            .withColumn("_record_hash", F.sha2(F.concat_ws("||", *df.columns), 256)))


def gravar(df, schema, tabela):
    nome = f"{CATALOGO}.{schema}.{tabela}"
    (df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(nome))
    return nome


resumo_bronze = []
for entidade, arquivo in ARQUIVOS.items():
    bruto = (spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv(f"{VOLUME}/{arquivo}"))
    nome = gravar(com_linhagem(bruto, entidade, arquivo), SCHEMA_BRONZE, entidade)
    resumo_bronze.append((entidade, spark.table(nome).count(), len(bruto.columns), nome))
    print(f"[BRONZE] {entidade:22s} {resumo_bronze[-1][1]:>9,} linhas -> {nome}".replace(",", "."))

# Microdados de aluno: já vêm em Parquet, então entram sem inferência de schema
alunos_bruto = spark.read.parquet(f"{VOLUME}/{ARQUIVO_ALUNOS}")
nome = gravar(com_linhagem(alunos_bruto, "aluno", ARQUIVO_ALUNOS), SCHEMA_BRONZE, "aluno")
resumo_bronze.append(("aluno", spark.table(nome).count(), len(alunos_bruto.columns), nome))
print(f"[BRONZE] {'aluno':22s} {resumo_bronze[-1][1]:>9,} linhas -> {nome}".replace(",", "."))

display(spark.createDataFrame(resumo_bronze, ["entidade", "linhas", "colunas", "tabela"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality gate da Bronze
# MAGIC
# MAGIC Mesmas dimensões do framework local (`src/quality/data_quality.py`), aqui em Spark puro para
# MAGIC não trazer os dados para o driver. Falha crítica interrompe o notebook, o que faz o job do
# MAGIC Lakeflow parar em vez de propagar dado ruim para a Silver.

# COMMAND ----------

# ============================================================
# VERIFICAÇÕES DE QUALIDADE
# ============================================================
REGRAS = {
    "indicador_municipio": [("min_count", None, 10000), ("not_null", "id_municipio", None),
                            ("not_null", "taxa_alfabetizacao", None),
                            ("chave", "ano,id_municipio,serie,rede", None)],
    "indicador_uf":        [("min_count", None, 100), ("not_null", "sigla_uf", None)],
    "meta_brasil":         [("min_count", None, 1), ("not_null", "ano", None)],
    "meta_uf":             [("min_count", None, 20), ("not_null", "sigla_uf", None)],
    "meta_municipio":      [("min_count", None, 10000), ("not_null", "id_municipio", None)],
    "dim_uf_ibge":         [("min_count", None, 27), ("unico", "id", None)],
    "dim_municipio_ibge":  [("min_count", None, 5500), ("unico", "id", None)],
    # O grão do aluno é (ano, id_aluno): o mesmo id reaparece no ciclo seguinte,
    # então unicidade isolada por id_aluno acusaria falso positivo.
    "aluno":               [("min_count", None, 1000), ("not_null", "id_aluno", None),
                            ("not_null", "id_municipio", None),
                            ("chave", "ano,id_aluno", None)],
}


def checar(entidade, regras):
    df = spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.{entidade}")
    linhas, falhas = [], []
    for tipo, coluna, valor in regras:
        if tipo == "min_count":
            total = df.count()
            ok, detalhe = total >= valor, f"{total} linhas (minimo {valor})"
        elif tipo == "not_null":
            nulos = df.filter(F.col(coluna).isNull()).count()
            ok, detalhe = nulos == 0, f"{nulos} nulos em {coluna}"
        elif tipo == "unico":
            dups = df.count() - df.select(coluna).distinct().count()
            ok, detalhe = dups == 0, f"{dups} duplicatas em {coluna}"
        elif tipo == "chave":
            chaves = coluna.split(",")
            dups = df.count() - df.select(*chaves).distinct().count()
            ok, detalhe = dups == 0, f"{dups} duplicatas no grao ({coluna})"
        linhas.append((entidade, tipo, coluna or "-", "PASS" if ok else "FAIL", detalhe))
        if not ok:
            falhas.append(f"{entidade}.{tipo}:{coluna} -> {detalhe}")
    return linhas, falhas


todas, todas_falhas = [], []
for entidade, regras in REGRAS.items():
    linhas, falhas = checar(entidade, regras)
    todas += linhas
    todas_falhas += falhas

display(spark.createDataFrame(todas, ["entidade", "verificacao", "coluna", "status", "detalhe"]))

if todas_falhas:
    raise Exception("[QUALITY GATE] Bronze reprovada:\n  - " + "\n  - ".join(todas_falhas))
print(f"Quality gate da Bronze: {len(todas)} verificacoes, todas aprovadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Camada Silver
# MAGIC
# MAGIC Padronização de tipos, decodificação do domínio `rede`, normalização de chaves, deduplicação
# MAGIC pelo grão e integração das bases. É o mesmo código do job Glue `cloud/aws/glue/etl_silver.py`,
# MAGIC mudando apenas a origem e o destino: tabelas do Unity Catalog no lugar de caminhos no S3.

# COMMAND ----------

# ============================================================
# DIMENSÕES TERRITORIAIS
# ============================================================
def ler_bronze(entidade):
    return spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.{entidade}")


def mapear_rede(coluna):
    expressao = F.lit(None).cast("string")
    for codigo, nome in DIM_REDE.items():
        expressao = F.when(coluna == codigo, F.lit(nome)).otherwise(expressao)
    return expressao


dim_uf = (ler_bronze("dim_uf_ibge")
          .select(F.col("id").cast("int").alias("id_uf"),
                  F.upper(F.trim(F.col("sigla"))).alias("sigla_uf"),
                  F.trim(F.col("nome")).alias("nome_uf"),
                  F.col("regiao_sigla").alias("sigla_regiao"),
                  F.col("regiao_nome").alias("nome_regiao"))
          .dropDuplicates(["id_uf"]))
gravar(dim_uf, SCHEMA_SILVER, "dim_uf")

# O código IBGE tem 7 dígitos e os 2 primeiros são a UF. Derivar daqui, em vez de
# confiar no aninhamento do JSON, protege contra município sem microrregião.
dim_municipio = (ler_bronze("dim_municipio_ibge")
                 .select(F.col("id").cast("int").alias("id_municipio"),
                         F.trim(F.col("nome")).alias("nome_municipio"),
                         F.col("microrregiao_nome").alias("nome_microrregiao"),
                         F.col("microrregiao_mesorregiao_nome").alias("nome_mesorregiao"))
                 .withColumn("id_uf", (F.col("id_municipio") / 100000).cast("int"))
                 .join(broadcast(dim_uf), on="id_uf", how="left")
                 .filter(F.col("sigla_uf").isNotNull()))
gravar(dim_municipio, SCHEMA_SILVER, "dim_municipio")

print(f"dim_uf: {dim_uf.count()} | dim_municipio: {dim_municipio.count()}")

# COMMAND ----------

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
gravar(fato_indicador_municipio, SCHEMA_SILVER, "fato_indicador_municipio")

fato_indicador_uf = (ler_bronze("indicador_uf")
    .withColumn("ano", F.col("ano").cast("int"))
    .withColumn("rede", F.col("rede").cast("int"))
    .withColumn("serie", F.col("serie").cast("int"))
    .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
    .withColumn("taxa_alfabetizacao", F.round(F.col("taxa_alfabetizacao").cast("double"), 2))
    .withColumn("media_portugues", F.round(F.col("media_portugues").cast("double"), 2))
    .withColumn("rede_nome", mapear_rede(F.col("rede")))
    .dropDuplicates(["ano", "sigla_uf", "serie", "rede"])
    .select("ano", "sigla_uf", "serie", "rede", "rede_nome",
            "taxa_alfabetizacao", "media_portugues")
    .join(broadcast(dim_uf.select("sigla_uf", "id_uf", "nome_uf", "nome_regiao")),
          on="sigla_uf", how="left"))
gravar(fato_indicador_uf, SCHEMA_SILVER, "fato_indicador_uf")

# As 9 colunas de proporção viram uma tabela longa (ano, município, rede, nível, proporção)
distribuicao = None
for nivel, coluna in enumerate(COLUNAS_NIVEL):
    parcial = (indicador_municipio
               .select("ano", "id_municipio", "rede", "rede_nome",
                       F.col(coluna).cast("double").alias("proporcao_alunos"))
               .withColumn("nivel", F.lit(nivel))
               .filter(F.col("proporcao_alunos").isNotNull()))
    distribuicao = parcial if distribuicao is None else distribuicao.unionByName(parcial)
gravar(distribuicao, SCHEMA_SILVER, "fato_distribuicao_nivel")

print(f"indicador municipio: {fato_indicador_municipio.count()} | "
      f"indicador uf: {fato_indicador_uf.count()} | "
      f"distribuicao por nivel: {distribuicao.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Microdados de aluno
# MAGIC
# MAGIC São 3,87 milhões de linhas do Inep, no grão de aluno. É a sexta entidade que o desafio pede.
# MAGIC A tabela não cabe no download gratuito do portal da Base dos Dados, então foi extraída do
# MAGIC BigQuery público e enviada ao volume em Parquet.
# MAGIC
# MAGIC Três colunas mudam a forma de calcular o indicador, e são a diferença entre reproduzir o
# MAGIC número oficial e chegar perto dele:
# MAGIC
# MAGIC | Coluna | Efeito |
# MAGIC |---|---|
# MAGIC | `presenca` / `preenchimento_caderno` | nem todo matriculado fez a prova, e só quem fez entra no cálculo |
# MAGIC | `peso_aluno` | peso amostral, então a taxa oficial é média ponderada e não simples |
# MAGIC | `alfabetizado` | já vem da fonte, então é preservado e conferido contra a regra dos 743 pontos |
# MAGIC
# MAGIC A célula de conferência reconstrói o indicador municipal a partir do grão de aluno e compara
# MAGIC com o número publicado. Se a regra estivesse errada, o erro apareceria ali.

# COMMAND ----------

# ============================================================
# SILVER DE MICRODADOS DE ALUNO
# ============================================================
aluno_bronze = ler_bronze("aluno")

# A fonte publica a coluna como 'proficiencia'; padronizamos o nome na Silver.
if "proficiencia" in aluno_bronze.columns:
    aluno_bronze = aluno_bronze.withColumnRenamed("proficiencia", "proficiencia_portugues")

fato_aluno = (aluno_bronze
    .withColumn("ano", F.col("ano").cast("int"))
    .withColumn("id_municipio", F.col("id_municipio").cast("int"))
    .withColumn("rede", F.col("rede").cast("int"))
    .withColumn("serie", F.col("serie").cast("int"))
    .withColumn("proficiencia_portugues", F.col("proficiencia_portugues").cast("double"))
    .withColumn("peso_aluno", F.coalesce(F.col("peso_aluno").cast("double"), F.lit(1.0)))
    # O registro vale mesmo sem prova: o ausente compõe o denominador da participação.
    .filter(F.col("id_aluno").isNotNull()
            & F.col("id_municipio").between(1000000, 9999999)
            & (F.col("proficiencia_portugues").isNull()
               | F.col("proficiencia_portugues").between(200, 1000)))
    # O grão é (ano, aluno): o mesmo id reaparece no ciclo seguinte.
    .dropDuplicates(["ano", "id_aluno"])
    .withColumn("avaliado", F.col("proficiencia_portugues").isNotNull())
    .withColumn("alfabetizado", F.col("alfabetizado").cast("double") == 1.0)
    .withColumn("rede_nome", mapear_rede(F.col("rede")))
    .withColumn("faixa_proficiencia",
                F.when(F.col("proficiencia_portugues") < 650, "Muito baixa")
                 .when(F.col("proficiencia_portugues") < 700, "Baixa")
                 .when(F.col("proficiencia_portugues") < PONTO_CORTE_SAEB, "Abaixo do corte")
                 .when(F.col("proficiencia_portugues") < 800, "Adequada")
                 .otherwise("Avancada"))
    .select("id_aluno", "ano", "id_municipio", "id_escola", "rede", "rede_nome", "serie",
            "proficiencia_portugues", "alfabetizado", "avaliado", "faixa_proficiencia",
            "presenca", "preenchimento_caderno", "peso_aluno"))

gravar_silver(fato_aluno, "fato_aluno")

total = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.fato_aluno").count()
avaliados = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.fato_aluno").filter("avaliado").count()
print(f"Alunos no cadastro     : {total:,}".replace(",", "."))
print(f"Efetivamente avaliados : {avaliados:,} ({avaliados/total*100:.1f}%)".replace(",", "."))

# COMMAND ----------

# ============================================================
# RECONSTRUÇÃO DO INDICADOR A PARTIR DO GRÃO DE ALUNO
# ============================================================
# Aplicando a regra oficial — média ponderada por peso_aluno, apenas entre os
# avaliados — temos que chegar ao mesmo número que o Inep publica.
avaliados_df = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.fato_aluno").filter("avaliado")

reconstruido = (avaliados_df
    .groupBy("ano", "id_municipio", "rede")
    .agg((F.sum(F.col("alfabetizado").cast("double") * F.col("peso_aluno"))
          / F.sum("peso_aluno") * 100).alias("taxa_reconstruida"),
         (F.sum(F.col("proficiencia_portugues") * F.col("peso_aluno"))
          / F.sum("peso_aluno")).alias("media_reconstruida"),
         F.count("*").alias("alunos")))

comparacao = (reconstruido
    .join(fato_indicador_municipio.select("ano", "id_municipio", "rede",
                                          "taxa_alfabetizacao", "media_portugues"),
          on=["ano", "id_municipio", "rede"], how="inner")
    .withColumn("erro_taxa", F.abs(F.col("taxa_reconstruida") - F.col("taxa_alfabetizacao")))
    .withColumn("erro_media", F.abs(F.col("media_reconstruida") - F.col("media_portugues")))
    .cache())

resumo = comparacao.agg(
    F.count("*").alias("combinacoes"),
    F.round(F.avg("erro_taxa"), 4).alias("erro_medio_taxa_pp"),
    F.round(F.avg("erro_media"), 4).alias("erro_medio_proficiencia"),
    F.round(F.avg((F.col("erro_taxa") < 0.05).cast("double")) * 100, 1).alias("pct_dentro_005"),
    F.round(F.abs(
        F.sum(F.col("taxa_reconstruida") * F.col("alunos")) / F.sum("alunos")
        - F.sum(F.col("taxa_alfabetizacao") * F.col("alunos")) / F.sum("alunos")), 4)
     .alias("erro_nacional_pp"),
).collect()[0]

print(f"Combinacoes comparadas (ano x municipio x rede): {resumo['combinacoes']:,}"
      .replace(",", "."))
print(f"Erro medio na taxa        : {resumo['erro_medio_taxa_pp']} p.p.")
print(f"Erro medio na proficiencia: {resumo['erro_medio_proficiencia']} pontos")
print(f"Dentro de 0,05 p.p.       : {resumo['pct_dentro_005']}% dos casos")
print(f"Erro no agregado nacional : {resumo['erro_nacional_pp']} p.p.")

if resumo["erro_nacional_pp"] > 0.5 or resumo["erro_medio_taxa_pp"] > 0.5:
    raise Exception("[QUALITY GATE] A reconstrucao do indicador nao bate com o publicado")
print("\nReconstrucao aprovada: os microdados reproduzem o indicador oficial.")

display(comparacao.orderBy(F.desc("erro_taxa")).limit(10))

# COMMAND ----------

# ============================================================
# TRAJETÓRIAS DE META (colunas por ano -> linhas)
# ============================================================
def normalizar_rede_texto(df):
    expressao = F.lit(None).cast("int")
    for texto, codigo in REDE_TEXTO.items():
        expressao = F.when(F.lower(F.trim(F.col("rede"))) == texto,
                           F.lit(codigo)).otherwise(expressao)
    return df.withColumn("rede", expressao)


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

    # A mesma meta aparece nas linhas de 2023 e 2024, às vezes revisada.
    # Mantemos a revisão declarada no ano de referência mais recente.
    janela = Window.partitionBy(*chaves, "rede", "ano_meta").orderBy(F.col("ano").desc())
    return (empilhados
            .withColumn("_ordem", F.row_number().over(janela))
            .filter(F.col("_ordem") == 1)
            .drop("_ordem", "ano")
            .withColumn("rede_nome", mapear_rede(F.col("rede"))))


meta_municipio = normalizar_rede_texto(
    ler_bronze("meta_municipio").withColumn("id_municipio", F.col("id_municipio").cast("int")))
gravar(trajetoria_metas(meta_municipio, ["id_municipio"]), SCHEMA_SILVER, "dim_meta_municipio")

meta_uf = normalizar_rede_texto(
    ler_bronze("meta_uf").withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf")))))
gravar(trajetoria_metas(meta_uf, ["sigla_uf"]), SCHEMA_SILVER, "dim_meta_uf")

# Participação e nível agregado só existem na tabela de metas municipais
participacao = (normalizar_rede_texto(
        ler_bronze("meta_municipio").withColumn("id_municipio", F.col("id_municipio").cast("int")))
    .select(F.col("ano").cast("int").alias("ano"), "id_municipio", "rede",
            F.col("percentual_participacao").cast("double").alias("percentual_participacao"),
            F.col("nivel_alfabetizacao").cast("double").alias("nivel_alfabetizacao"))
    .filter(F.col("percentual_participacao").isNotNull())
    .dropDuplicates(["ano", "id_municipio", "rede"]))
gravar(participacao, SCHEMA_SILVER, "fato_participacao_municipio")

print("Trajetorias de meta e participacao gravadas.")

# COMMAND ----------

# ============================================================
# TABELA INTEGRADA
# ============================================================
metas_municipio = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.dim_meta_municipio")

meta_2030 = (metas_municipio.filter(F.col("ano_meta") == 2030)
             .select("id_municipio", "rede", F.col("meta_taxa").alias("meta_taxa_2030")))

contexto_uf = (fato_indicador_uf
               .select("ano", "sigla_uf", "rede", F.col("taxa_alfabetizacao").alias("taxa_uf")))

contexto_br = (ler_bronze("meta_brasil")
               .select(F.col("ano").cast("int").alias("ano"),
                       F.col("taxa_alfabetizacao").cast("double").alias("taxa_brasil_publica")))

integrada = (fato_indicador_municipio
    .join(broadcast(dim_municipio.select("id_municipio", "nome_municipio", "sigla_uf",
                                         "nome_uf", "nome_regiao", "nome_mesorregiao")),
          on="id_municipio", how="left")
    .join(metas_municipio.select("id_municipio", "rede",
                                 F.col("ano_meta").alias("ano"),
                                 F.col("meta_taxa").alias("meta_taxa_ano")),
          on=["id_municipio", "rede", "ano"], how="left")
    .join(broadcast(meta_2030), on=["id_municipio", "rede"], how="left")
    .join(participacao, on=["ano", "id_municipio", "rede"], how="left")
    .join(contexto_uf, on=["ano", "sigla_uf", "rede"], how="left")
    .join(broadcast(contexto_br), on="ano", how="left")
    .withColumn("meta_taxa_2030", F.coalesce(F.col("meta_taxa_2030"), F.lit(META_NACIONAL_2030)))
    .withColumn("gap_meta_ano", F.round(F.col("taxa_alfabetizacao") - F.col("meta_taxa_ano"), 2))
    .withColumn("atingiu_meta_ano", F.col("gap_meta_ano") >= 0)
    .withColumn("gap_meta_2030", F.round(F.col("meta_taxa_2030") - F.col("taxa_alfabetizacao"), 2))
    .withColumn("dif_vs_uf", F.round(F.col("taxa_alfabetizacao") - F.col("taxa_uf"), 2))
    .withColumn("dif_vs_brasil",
                F.round(F.col("taxa_alfabetizacao") - F.col("taxa_brasil_publica"), 2)))

# Gate da Silver: o grão precisa ser único e todo município precisa ter território
total = integrada.count()
duplicadas = total - integrada.dropDuplicates(["ano", "id_municipio", "rede"]).count()
sem_territorio = integrada.filter(F.col("sigla_uf").isNull()).count()
print(f"[DQ:SILVER] linhas={total} duplicadas={duplicadas} sem_territorio={sem_territorio}")
if duplicadas or sem_territorio:
    raise Exception(f"[QUALITY GATE] Silver integrada reprovada: {duplicadas} duplicatas, "
                    f"{sem_territorio} linhas sem territorio")

gravar(integrada, SCHEMA_SILVER, "fato_alfabetizacao_municipio")
display(integrada.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consistência entre fontes
# MAGIC
# MAGIC Esta verificação prova o mapeamento de `rede`. A taxa da rede Municipal no indicador tem que
# MAGIC bater com a taxa da tabela de metas municipais. Se o dicionário estivesse errado, daria
# MAGIC divergência em massa. A tolerância de 0,1 p.p. existe porque as duas fontes oficiais arredondam
# MAGIC de forma diferente em 2023.

# COMMAND ----------

# ============================================================
# INDICADOR (rede Municipal) x TABELA DE METAS
# ============================================================
esquerda = (integrada.filter(F.col("rede") == 3)
            .select("ano", "id_municipio", "taxa_alfabetizacao"))
direita = (ler_bronze("meta_municipio")
           .select(F.col("ano").cast("int").alias("ano"),
                   F.col("id_municipio").cast("int").alias("id_municipio"),
                   F.col("taxa_alfabetizacao").cast("double").alias("taxa_fonte_meta")))

cruzamento = esquerda.join(direita, on=["ano", "id_municipio"], how="inner").dropna()
divergentes = cruzamento.filter(
    F.abs(F.col("taxa_alfabetizacao") - F.col("taxa_fonte_meta")) > 0.1)

comparados = cruzamento.count()
n_divergentes = divergentes.count()
pct = n_divergentes / comparados * 100 if comparados else 0

print(f"Comparados: {comparados} | divergentes: {n_divergentes} ({pct:.3f}%)")
if pct >= 1.0:
    raise Exception(f"[QUALITY GATE] Mapeamento de rede suspeito: {pct:.2f}% de divergencia")

print("Mapeamento rede=3 -> Municipal confirmado pelos dados.")
if n_divergentes:
    print(f"\nAs {n_divergentes} divergencias abaixo sao inconsistencias da propria fonte "
          f"e seriam reportadas ao produtor do dado:")
    display(divergentes)

# COMMAND ----------

# ============================================================
# RESUMO
# ============================================================
tabelas = spark.sql(f"SHOW TABLES IN {CATALOGO}.{SCHEMA_SILVER}").collect()
linhas = [(t.tableName, spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.{t.tableName}").count())
          for t in tabelas]

print(f"SILVER: {len(linhas)} tabelas\n")
display(spark.createDataFrame(linhas, ["tabela", "linhas"]))
print("\nProximo: 02_streaming (ingestao de eventos) e 03_gold (camada analitica).")
