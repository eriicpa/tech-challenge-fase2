# Databricks notebook source
# MAGIC %md
# MAGIC # Tech Challenge - Fase 2 | Databricks
# MAGIC ## Notebook 03 - Camada Gold
# MAGIC
# MAGIC > Pré-requisitos: `01_bronze_silver` e `02_streaming` executados.
# MAGIC
# MAGIC Este notebook não tem SQL escrito nele. Ele importa `src/gold/consultas.py`, o mesmo arquivo
# MAGIC que o notebook local e o job do AWS Glue usam, e executa as consultas aqui.
# MAGIC
# MAGIC É o retorno prático de ter escrito a Gold em SQL padrão em vez da DSL do Spark: a regra de
# MAGIC negócio tem uma fonte só, versionada no Git, e já rodou em três motores diferentes, sendo
# MAGIC DuckDB no desenvolvimento, Spark local e agora Databricks. Se a definição de risco crítico
# MAGIC mudar, muda em um lugar.
# MAGIC
# MAGIC Para o `import` funcionar, o repositório precisa ter sido adicionado como Git folder.

# COMMAND ----------

# ============================================================
# CONFIGURAÇÃO E IMPORT DAS CONSULTAS
# ============================================================
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
dbutils.widgets.text("schema_bronze", "tc2_bronze", "Schema Bronze")
dbutils.widgets.text("schema_silver", "tc2_silver", "Schema Silver")
dbutils.widgets.text("schema_gold", "tc2_gold", "Schema Gold")
dbutils.widgets.text("schema_landing", "tc2_landing", "Schema Landing")
dbutils.widgets.text("volume", "arquivos", "Volume")
dbutils.widgets.text("tabelas", "todas", "Tabelas a gerar")

CATALOGO = dbutils.widgets.get("catalogo")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
SCHEMA_SILVER = dbutils.widgets.get("schema_silver")
SCHEMA_GOLD = dbutils.widgets.get("schema_gold")
SCHEMA_LANDING = dbutils.widgets.get("schema_landing")
VOLUME = f"/Volumes/{CATALOGO}/{SCHEMA_LANDING}/{dbutils.widgets.get('volume')}"
SELECAO_TABELAS = dbutils.widgets.get("tabelas")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA_GOLD}")
RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def localizar_raiz_repositorio():
    inicio = Path(os.getcwd()).resolve()
    for candidato in [inicio, *inicio.parents]:
        if (candidato / "src" / "gold" / "consultas.py").exists():
            return candidato
    return None


RAIZ = localizar_raiz_repositorio()
if RAIZ is None:
    raise Exception(
        "Nao encontrei src/gold/consultas.py.\n"
        "Este notebook precisa rodar de dentro do repositorio importado como Git folder:\n"
        "  Workspace > Create > Git folder > URL do repositorio\n"
        "Depois abra cloud/databricks/03_gold a partir da pasta criada."
    )

sys.path.insert(0, str(RAIZ))
from src.gold.consultas import CONSULTAS_GOLD

print(f"Repositorio: {RAIZ}")
print(f"Consultas carregadas: {len(CONSULTAS_GOLD)}")
for nome, spec in CONSULTAS_GOLD.items():
    print(f"  - {nome:32s} {spec['pergunta']}")

# COMMAND ----------

# ============================================================
# VIEWS DA SILVER
# ============================================================
# As consultas esperam nomes genéricos (silver_*), que aqui apontam para as
# tabelas Delta do Unity Catalog. É o mesmo mecanismo do notebook local, onde
# apontavam para pastas Parquet.
VIEWS = {
    "silver_alfabetizacao_municipio": f"{CATALOGO}.{SCHEMA_SILVER}.fato_alfabetizacao_municipio",
    "silver_indicador_uf":            f"{CATALOGO}.{SCHEMA_SILVER}.fato_indicador_uf",
    "silver_dim_municipio":           f"{CATALOGO}.{SCHEMA_SILVER}.dim_municipio",
    "silver_dim_uf":                  f"{CATALOGO}.{SCHEMA_SILVER}.dim_uf",
    "silver_meta_municipio":          f"{CATALOGO}.{SCHEMA_SILVER}.dim_meta_municipio",
    "silver_meta_uf":                 f"{CATALOGO}.{SCHEMA_SILVER}.dim_meta_uf",
    "silver_distribuicao_nivel":      f"{CATALOGO}.{SCHEMA_SILVER}.fato_distribuicao_nivel",
    "silver_aluno":                   f"{CATALOGO}.{SCHEMA_SILVER}.fato_aluno",
    "silver_avaliacao_stream":        f"{CATALOGO}.{SCHEMA_SILVER}.fato_avaliacao_stream",
}

registradas = set()
for view, tabela in VIEWS.items():
    if spark.catalog.tableExists(tabela):
        spark.sql(f"CREATE OR REPLACE TEMP VIEW {view} AS SELECT * FROM {tabela}")
        registradas.add(view)
        print(f"{view:34s} -> {tabela}")
    else:
        print(f"{view:34s} -- ausente, consultas dependentes serao puladas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construção da Gold
# MAGIC
# MAGIC Cada consulta vira uma tabela Delta em `tc2_gold`. Sem particionamento físico, pelo mesmo
# MAGIC motivo do notebook 01: nesse volume o data skipping do Delta resolve, e particionar só criaria
# MAGIC arquivos pequenos.

# COMMAND ----------

# ============================================================
# EXECUÇÃO DAS CONSULTAS
# ============================================================
import time

resultados = []
alvos = (list(CONSULTAS_GOLD) if SELECAO_TABELAS.strip().lower() == "todas"
         else [t.strip() for t in SELECAO_TABELAS.split(",") if t.strip()])

for nome in alvos:
    spec = CONSULTAS_GOLD[nome]
    dependencias = [v for v in VIEWS if v in spec["sql"]]
    ausentes = [v for v in dependencias if v not in registradas]
    if ausentes:
        print(f"[GOLD] {nome:32s} PULADA (falta {', '.join(ausentes)})")
        continue

    inicio = time.perf_counter()
    try:
        df = spark.sql(spec["sql"])
        destino = f"{CATALOGO}.{SCHEMA_GOLD}.{nome}"
        (df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(destino))
        linhas = spark.table(destino).count()
        duracao = time.perf_counter() - inicio
        resultados.append((nome, linhas, round(duracao, 2), spec["pergunta"]))
        print(f"[GOLD] {nome:32s} {linhas:>7,} linhas | {duracao:5.2f}s".replace(",", "."))
    except Exception as exc:
        if spec.get("opcional"):
            print(f"[GOLD] {nome:32s} FALHOU (opcional): {exc}")
        else:
            raise

display(spark.createDataFrame(resultados, ["tabela", "linhas", "segundos", "pergunta"]))

# COMMAND ----------

# ============================================================
# QUALITY GATE DA GOLD
# ============================================================
# A Gold é o que chega ao gestor e ao modelo, então ela também passa por gate.
indicador = spark.table(f"{CATALOGO}.{SCHEMA_GOLD}.gold_indicador_municipio")

verificacoes = []
total = indicador.count()
verificacoes.append(("row_count", total >= 10000, f"{total} linhas"))

dups = total - indicador.select("ano", "id_municipio", "rede").distinct().count()
verificacoes.append(("chave_composta", dups == 0, f"{dups} duplicatas no grao"))

nulos = indicador.filter(F.col("nome_municipio").isNull() | F.col("sigla_uf").isNull()).count()
verificacoes.append(("not_null:territorio", nulos == 0, f"{nulos} linhas sem territorio"))

fora_dominio = indicador.filter(
    ~F.col("risco_alfabetizacao").isin("Critico", "Alto", "Medio", "Baixo")).count()
verificacoes.append(("dominio:risco", fora_dominio == 0, f"{fora_dominio} fora do dominio"))

fora_faixa = indicador.filter(~F.col("taxa_alfabetizacao").between(0, 100)).count()
verificacoes.append(("faixa:taxa", fora_faixa == 0, f"{fora_faixa} fora de [0,100]"))

# Consistência entre ciclos: uma UF que se move de forma incompatível com o país
# indica mudança de metodologia na origem, não queda de aprendizagem.
evolucao = spark.table(f"{CATALOGO}.{SCHEMA_GOLD}.gold_evolucao_temporal").filter(
    (F.col("rede_nome") == "Municipal") & F.col("variacao_pp").isNotNull())
mediana = evolucao.approxQuantile("variacao_pp", [0.5], 0.01)[0]
por_uf = evolucao.groupBy("sigla_uf").agg(F.avg("variacao_pp").alias("media"))
anomalas = por_uf.filter(F.abs(F.col("media") - F.lit(mediana)) > 15).collect()

falhas = [f"{nome}: {detalhe}" for nome, ok, detalhe in verificacoes if not ok]
display(spark.createDataFrame(
    [(n, "PASS" if ok else "FAIL", d) for n, ok, d in verificacoes],
    ["verificacao", "status", "detalhe"]))

if anomalas:
    lista = ", ".join(f"{r['sigla_uf']} ({r['media']:+.1f} p.p.)" for r in anomalas)
    print(f"\n[AVISO] Possivel quebra de serie historica em: {lista}")
    print(f"        Mediana nacional: {mediana:+.2f} p.p.")
    print("        Tratado explicitamente na analise (Notebook 04 local).")

if falhas:
    raise Exception("[QUALITY GATE] Gold reprovada:\n  - " + "\n  - ".join(falhas))
print(f"\nQuality gate da Gold: {len(verificacoes)} verificacoes aprovadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Otimização das tabelas
# MAGIC
# MAGIC `OPTIMIZE` compacta arquivos pequenos e o `ZORDER` reorganiza os dados pelas colunas mais
# MAGIC filtradas, melhorando o data skipping. É o equivalente Delta ao particionamento feito no S3,
# MAGIC com a vantagem de não engessar o layout numa coluna só.

# COMMAND ----------

# ============================================================
# OPTIMIZE + ZORDER
# ============================================================
OTIMIZAR = {
    "gold_indicador_municipio": "ano, sigla_uf",
    "gold_meta_vs_realizado": "ano, sigla_uf",
    "gold_evolucao_temporal": "sigla_uf",
}

for tabela, colunas in OTIMIZAR.items():
    nome = f"{CATALOGO}.{SCHEMA_GOLD}.{tabela}"
    try:
        spark.sql(f"OPTIMIZE {nome} ZORDER BY ({colunas})")
        print(f"OPTIMIZE {tabela:32s} ZORDER BY ({colunas})")
    except Exception as exc:
        print(f"OPTIMIZE {tabela:32s} indisponivel neste workspace: {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consultas de negócio
# MAGIC
# MAGIC Quatro perguntas que a camada Gold responde em SQL direto, sem transformação adicional:
# MAGIC
# MAGIC 1. Onde intervir primeiro — municípios em risco crítico, do pior para o melhor
# MAGIC 2. Quais estados estão na rota da meta e quais concentram municípios críticos
# MAGIC 3. Quanto cada região precisa avançar por ano para chegar em 2030
# MAGIC 4. Municípios que atingiram a meta do ano mas estão abaixo da média da própria UF
# MAGIC
# MAGIC As definições são as mesmas de `cloud/aws/athena/ddl_gold.sql`, então o SQL roda sem
# MAGIC alteração no Athena ou no SQL editor do Databricks.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Municípios em risco crítico (prioridade de intervenção)
# MAGIC SELECT sigla_uf, nome_municipio, taxa_alfabetizacao, meta_taxa_ano, gap_meta_2030
# MAGIC FROM workspace.tc2_gold.gold_indicador_municipio
# MAGIC WHERE ano = 2024 AND rede_nome = 'Municipal' AND risco_alfabetizacao = 'Critico'
# MAGIC ORDER BY taxa_alfabetizacao ASC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. Ranking de estados por percentual de municípios dentro da meta
# MAGIC SELECT sigla_uf, nome_regiao, municipios, taxa_media,
# MAGIC        pct_municipios_na_meta, municipios_criticos
# MAGIC FROM workspace.tc2_gold.gold_meta_vs_realizado
# MAGIC WHERE ano = 2024 AND rede_nome = 'Municipal'
# MAGIC ORDER BY pct_municipios_na_meta DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. Esforço anual necessário até 2030, por região
# MAGIC SELECT nome_regiao,
# MAGIC        COUNT(*) AS municipios,
# MAGIC        ROUND(AVG(taxa_alfabetizacao), 2) AS taxa_media,
# MAGIC        ROUND(AVG(ritmo_anual_necessario_pp), 2) AS ritmo_anual_necessario_pp
# MAGIC FROM workspace.tc2_gold.gold_indicador_municipio
# MAGIC WHERE ano = 2024 AND rede_nome = 'Municipal'
# MAGIC GROUP BY nome_regiao
# MAGIC ORDER BY ritmo_anual_necessario_pp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4. Bom no absoluto, ruim no relativo: atingiu a meta do ano mas está abaixo da média da UF
# MAGIC SELECT sigla_uf, nome_municipio, taxa_alfabetizacao, taxa_uf, dif_vs_uf
# MAGIC FROM workspace.tc2_gold.gold_indicador_municipio
# MAGIC WHERE ano = 2024 AND rede_nome = 'Municipal'
# MAGIC   AND situacao_meta = 'Meta atingida' AND dif_vs_uf < 0
# MAGIC ORDER BY dif_vs_uf ASC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## FinOps
# MAGIC
# MAGIC FinOps aqui não é escolher a máquina mais barata, e sim entender o que é cobrado e desenhar
# MAGIC a pipeline em torno disso.
# MAGIC
# MAGIC | Serviço | Unidade cobrada | Alavanca de economia |
# MAGIC |---|---|---|
# MAGIC | S3 | GB armazenado por mês | formato comprimido e ciclo de vida por camada |
# MAGIC | Athena | TB escaneado | particionamento, projeção de colunas e Parquet |
# MAGIC | Glue | DPU-hora | menos shuffle, menos releitura, job dimensionado |
# MAGIC | MSK | hora de broker | streaming ligado só na janela em que há evento |
# MAGIC
# MAGIC A alavanca dominante é a segunda, porque o Athena cobra por byte lido e não por linha
# MAGIC devolvida. A célula abaixo mede isso nos próprios dados: a mesma pergunta respondida de
# MAGIC quatro formas, com os bytes que cada uma obriga a ler.

# COMMAND ----------

# ============================================================
# BYTES ESCANEADOS: A MESMA PERGUNTA, QUATRO CUSTOS
# ============================================================
# Pergunta: "taxa de alfabetizacao por municipio na rede Municipal em 2024?"
# Sao 4 colunas de 1 ano. A medicao abaixo mostra quantos bytes cada estrategia
# obriga a ler, que e exatamente o que o Athena cobra.
import pyarrow.parquet as pq

COLUNAS_NECESSARIAS = {"id_municipio", "nome_municipio", "taxa_alfabetizacao", "rede_nome"}
ANO_ALVO = 2024
DIR_MEDICAO = f"{VOLUME}/_finops/gold_indicador_municipio"


def formatar_bytes(n):
    for unidade in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unidade}"
        n /= 1024
    return f"{n:.1f} TB"


def bytes_das_colunas(caminho, colunas=None):
    """Bytes realmente lidos ao projetar apenas algumas colunas.

    Le o rodape do Parquet, que guarda o tamanho comprimido de cada coluna em
    cada row group. E o mesmo mecanismo que faz o Athena cobrar so pelo lido.
    """
    total = 0
    for arquivo in sorted(Path(caminho).rglob("*.parquet")):
        metadados = pq.ParquetFile(arquivo).metadata
        nomes = [metadados.schema.column(i).name for i in range(metadados.num_columns)]
        for grupo in range(metadados.num_row_groups):
            for indice, nome in enumerate(nomes):
                if colunas is None or nome in colunas:
                    total += metadados.row_group(grupo).column(indice).total_compressed_size
    return total


# A Gold vive em Delta gerenciado, cujos arquivos nao sao acessiveis pelo sistema
# de arquivos. Para medir bytes reais, a mesma tabela e materializada em Parquet
# particionado no volume — que e o formato que estaria no S3 sob o Athena.
(spark.table(f"{CATALOGO}.{SCHEMA_GOLD}.gold_indicador_municipio")
 .write.mode("overwrite").partitionBy("ano").parquet(DIR_MEDICAO))

csv_origem = f"{VOLUME}/br_inep_avaliacao_alfabetizacao_municipio.csv"
bytes_csv = [a.size for a in dbutils.fs.ls(csv_origem)][0]

bytes_parquet_total = bytes_das_colunas(DIR_MEDICAO)
particao_alvo = f"{DIR_MEDICAO}/ano={ANO_ALVO}"
bytes_particao = bytes_das_colunas(particao_alvo)
bytes_otimizado = bytes_das_colunas(particao_alvo, COLUNAS_NECESSARIAS)

PRECO_ATHENA_POR_TB = 5.00
CONSULTAS_POR_MES = 500


def custo_athena(bytes_lidos, consultas=CONSULTAS_POR_MES):
    return bytes_lidos * consultas / (1024 ** 4) * PRECO_ATHENA_POR_TB


estrategias = [
    ("1. CSV, varredura completa", bytes_csv),
    ("2. Parquet sem particionamento", bytes_parquet_total),
    (f"3. Parquet + particao (ano={ANO_ALVO})", bytes_particao),
    ("4. Parquet + particao + projecao de colunas", bytes_otimizado),
]
tabela_estrategias = spark.createDataFrame(
    [(nome, formatar_bytes(b), f"{b / bytes_csv * 100:.1f}%", round(custo_athena(b), 4))
     for nome, b in estrategias],
    ["estrategia", "lido", "vs_csv", f"custo_athena_{CONSULTAS_POR_MES}_consultas_usd"])

print("BYTES LIDOS PARA RESPONDER A MESMA PERGUNTA")
display(tabela_estrategias)

fator = bytes_csv / max(bytes_otimizado, 1)
print(f"A estrategia 4 le {fator:.0f}x menos bytes que a 1, com o mesmo resultado.")
print("No Athena, essa e a razao entre as duas contas no fim do mes.")

dbutils.fs.rm(f"{VOLUME}/_finops", True)   # a copia so existia para a medicao

# COMMAND ----------

# ============================================================
# ESTIMATIVA DE CUSTO MENSAL DA ARQUITETURA
# ============================================================
# Precos de referencia us-east-1, de tabela publica. Devem ser reconferidos no
# AWS Pricing Calculator antes de virarem orcamento.
PRECOS = {
    "s3_standard_gb_mes":     0.023,
    "s3_standard_ia_gb_mes":  0.0125,
    "s3_glacier_ir_gb_mes":   0.004,
    "glue_dpu_hora":          0.44,
    "msk_serverless_hora":    0.75,
    "cloudwatch_metrica_mes": 0.30,
}

# Premissas de producao. O dado oficial e anual, mas a operacao nao e: revisoes
# de meta e correcoes da fonte chegam ao longo do ano.
PREMISSAS = {
    "execucoes_batch_mes":   4,     # uma por semana, para capturar republicacoes
    "dpus_por_execucao":     2,
    "horas_por_execucao":    0.25,
    "semanas_streaming_ano": 6,     # janela de aplicacao da avaliacao
    "consultas_athena_mes":  CONSULTAS_POR_MES,
    "metricas_cloudwatch":   10,
}


def bytes_do_schema(schema):
    """Soma o tamanho fisico das tabelas Delta de um schema."""
    total = 0
    for t in spark.sql(f"SHOW TABLES IN {CATALOGO}.{schema}").collect():
        if t.isTemporary:
            continue
        detalhe = spark.sql(f"DESCRIBE DETAIL {CATALOGO}.{schema}.{t.tableName}").collect()[0]
        total += detalhe["sizeInBytes"] or 0
    return total


gb_bronze = bytes_do_schema(SCHEMA_BRONZE) / (1024 ** 3)
gb_silver = bytes_do_schema(SCHEMA_SILVER) / (1024 ** 3)
gb_gold = bytes_do_schema(SCHEMA_GOLD) / (1024 ** 3)

custo_glue = (PREMISSAS["execucoes_batch_mes"] * PREMISSAS["dpus_por_execucao"]
              * PREMISSAS["horas_por_execucao"] * PRECOS["glue_dpu_hora"])
custo_athena_mes = custo_athena(bytes_otimizado, PREMISSAS["consultas_athena_mes"])
horas_msk_mes = PREMISSAS["semanas_streaming_ano"] * 7 * 24 / 12
custo_msk = horas_msk_mes * PRECOS["msk_serverless_hora"]

componentes = [
    ("S3 Bronze (Glacier IR apos 90 dias)", f"{gb_bronze:.2f} GB",
     gb_bronze * PRECOS["s3_glacier_ir_gb_mes"]),
    ("S3 Silver (Standard-IA apos 30 dias)", f"{gb_silver:.2f} GB",
     gb_silver * PRECOS["s3_standard_ia_gb_mes"]),
    ("S3 Gold (Standard, sempre quente)", f"{gb_gold:.2f} GB",
     gb_gold * PRECOS["s3_standard_gb_mes"]),
    ("AWS Glue (ETL batch)",
     f"{PREMISSAS['execucoes_batch_mes']}x {PREMISSAS['dpus_por_execucao']} DPU "
     f"x {PREMISSAS['horas_por_execucao']}h", custo_glue),
    ("Amazon Athena (consultas analiticas)",
     f"{PREMISSAS['consultas_athena_mes']} consultas otimizadas", custo_athena_mes),
    ("Amazon MSK Serverless (janela de aplicacao)",
     f"{horas_msk_mes:.0f} h/mes (media anual)", custo_msk),
    ("CloudWatch (metricas e alarmes)",
     f"{PREMISSAS['metricas_cloudwatch']} metricas",
     PREMISSAS["metricas_cloudwatch"] * PRECOS["cloudwatch_metrica_mes"]),
]
total_mes = sum(c[2] for c in componentes)

print("ESTIMATIVA DE CUSTO MENSAL DA ARQUITETURA EQUIVALENTE NA AWS")
display(spark.createDataFrame([(n, d, round(c, 4)) for n, d, c in componentes],
                              ["componente", "dimensionamento", "custo_mes_usd"]))
print(f"TOTAL ESTIMADO: US$ {total_mes:.2f}/mes  (~US$ {total_mes * 12:.2f}/ano)")

# O contraste que justifica a arquitetura hibrida
msk_sempre_ligado = 730 * PRECOS["msk_serverless_hora"]
economia_anual = (msk_sempre_ligado - custo_msk) * 12
print(f"\nCom o streaming ligado o ano inteiro: US$ {msk_sempre_ligado:.2f}/mes so de MSK.")
print(f"Ligar o broker apenas na janela de aplicacao economiza "
      f"US$ {economia_anual:,.2f}/ano".replace(",", "."))
print("E a maior decisao de FinOps do projeto, e ela e arquitetural, nao de configuracao.")

# COMMAND ----------

# ============================================================
# INVENTÁRIO FINAL
# ============================================================
linhas = []
for schema in (SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD):
    # isTemporary filtra as views de sessao criadas acima, que aparecem no
    # SHOW TABLES de qualquer schema mas nao pertencem a nenhum.
    for t in spark.sql(f"SHOW TABLES IN {CATALOGO}.{schema}").collect():
        if t.isTemporary:
            continue
        nome = f"{CATALOGO}.{schema}.{t.tableName}"
        linhas.append((schema, t.tableName, spark.table(nome).count()))

inventario = spark.createDataFrame(linhas, ["camada", "tabela", "linhas"])
print(f"{inventario.count()} tabelas | "
      f"{inventario.agg(F.sum('linhas')).collect()[0][0]:,} linhas no total".replace(",", "."))
display(inventario.orderBy("camada", "tabela"))

print("\nCamadas:")
print(f"  {SCHEMA_BRONZE}: dados como vieram da fonte, com metadados de linhagem")
print(f"  {SCHEMA_SILVER}: limpos, padronizados e integrados, no grao de municipio e de aluno")
print(f"  {SCHEMA_GOLD}: agregados por pergunta de negocio, prontos para BI e modelagem")
