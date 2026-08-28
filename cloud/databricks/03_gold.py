# Databricks notebook source
# MAGIC %md
# MAGIC # Tech Challenge — Fase 2 | Databricks
# MAGIC ## Notebook 03 — Camada Gold
# MAGIC
# MAGIC > Pré-requisitos: `01_bronze_silver` e `02_streaming` executados.
# MAGIC
# MAGIC Este notebook **não tem SQL escrito nele**. Ele importa `src/gold/consultas.py` — o mesmo
# MAGIC arquivo que o notebook local e o job do AWS Glue usam — e executa as consultas aqui.
# MAGIC
# MAGIC É o retorno prático de ter escrito a Gold em SQL padrão em vez da DSL do Spark: a regra de
# MAGIC negócio tem **uma fonte só**, versionada no Git, e já rodou em três motores diferentes
# MAGIC (DuckDB no desenvolvimento, Spark local e agora Databricks). Se a definição de "risco crítico"
# MAGIC mudar, muda em um lugar.
# MAGIC
# MAGIC Para o `import` funcionar, o repositório precisa ter sido adicionado como **Git folder**
# MAGIC (ver `docs/guia_deploy_databricks.md`).

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
dbutils.widgets.text("tabelas", "todas", "Tabelas a gerar")

CATALOGO = dbutils.widgets.get("catalogo")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
SCHEMA_SILVER = dbutils.widgets.get("schema_silver")
SCHEMA_GOLD = dbutils.widgets.get("schema_gold")
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
# MAGIC motivo do notebook 01: nesse volume, o *data skipping* do Delta resolve e particionar só criaria
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
# MAGIC filtradas, melhorando o *data skipping*. É o equivalente Delta ao particionamento que fazemos
# MAGIC no S3 — com a vantagem de não engessar o layout numa coluna só.

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
# MAGIC As mesmas quatro consultas que estão em `cloud/aws/athena/ddl_gold.sql`, rodando aqui.
# MAGIC São elas que geram as evidências para a entrega — e as mesmas podem ser coladas no SQL editor
# MAGIC do Databricks para montar um dashboard.

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

# ============================================================
# INVENTÁRIO FINAL
# ============================================================
linhas = []
for schema in (SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD):
    for t in spark.sql(f"SHOW TABLES IN {CATALOGO}.{schema}").collect():
        nome = f"{CATALOGO}.{schema}.{t.tableName}"
        linhas.append((schema, t.tableName, spark.table(nome).count()))

inventario = spark.createDataFrame(linhas, ["camada", "tabela", "linhas"])
print(f"{inventario.count()} tabelas | "
      f"{inventario.agg(F.sum('linhas')).collect()[0][0]:,} linhas no total".replace(",", "."))
display(inventario.orderBy("camada", "tabela"))

print("\nEvidencias para a entrega:")
print("  - print desta tabela de inventario")
print("  - print do Catalog Explorer mostrando os 3 schemas")
print("  - print das 4 consultas de negocio acima")
print("  - print do job no Lakeflow com as 3 tarefas concluidas")
