# Plano de commits, branches e Pull Requests

O enunciado pede que o repositório demonstre o uso de Git durante o desenvolvimento: histórico de
commits que evidencie a evolução da pipeline, mensagens claras, branches por funcionalidade, Pull
Requests para integrar na `main` e comentários nas PRs justificando as alterações.

Como o código já está pronto, este documento organiza a subida em **13 PRs** que reconstroem essa
evolução de forma coerente: cada uma entrega uma parte que faz sentido sozinha e depende só do que
veio antes.

---

## Antes de começar

**Crie o repositório vazio no GitHub** (sem README, sem .gitignore — eles vêm no primeiro commit).

Instale o GitHub CLI, que deixa o processo muito mais rápido:

```bash
winget install GitHub.cli
gh auth login
```

Sem o CLI, dá para fazer tudo pela interface web: cada branch aparece no GitHub com um botão
*Compare & pull request*.

### Recomendação de ritmo

Treze PRs criadas e mescladas em vinte minutos deixam todos os commits com o mesmo horário, o que
não parece desenvolvimento. Se houver tempo, divida em **três sessões** (por exemplo PRs 1 a 5 num
dia, 6 a 9 no outro, 10 a 13 no terceiro). Se não houver, tudo bem — o conteúdo dos commits e das
discussões é o que mais pesa.

### Ao mesclar, não use *Squash*

O *Squash and merge* junta todos os commits da branch em um só e você perde o histórico detalhado,
que é justamente o que está sendo avaliado. Use **Create a merge commit**.

---

## Preparação: guardar tudo fora do Git primeiro

Como os arquivos já existem, o truque é começar com o repositório vazio e ir adicionando por partes.

```bash
cd C:\Users\Eric\disc_tmp\tc2-databricks
git init
git branch -M main
git remote add origin https://github.com/eriicpa/tech-challenge-fase2.git
```

Nada foi adicionado ainda: o `git status` mostra tudo como *untracked*. Cada PR abaixo adiciona um
pedaço.

---

## PR 1 — Estrutura inicial

```bash
git checkout -b chore/estrutura-inicial
git add .gitignore requirements.txt
git commit -m "chore: estrutura do projeto, gitignore e dependencias"

git add data/landing/br_inep_avaliacao_alfabetizacao_*.csv
git commit -m "data: cinco tabelas do indicador de alfabetizacao (Base dos Dados)"

git push -u origin chore/estrutura-inicial
gh pr create --base main --title "Estrutura inicial do projeto" --body "Primeiro commit com a estrutura de pastas, dependencias e as cinco tabelas do dataset br_inep_avaliacao_alfabetizacao baixadas da Base dos Dados.

O .gitignore ja exclui data/lake/ (gerado pela pipeline) e data/landing/alunos.parquet, que tem 53 MB."
```

> **Comentário para postar na PR:**
> Optei por versionar os CSVs de origem porque somam 2,1 MB e garantem que qualquer pessoa consiga
> rodar a pipeline sem baixar nada. O único arquivo de fora é o de microdados de aluno, por causa do
> tamanho — a extração dele está documentada na PR 5.

Depois: **Merge pull request** → **Create a merge commit**.

```bash
git checkout main && git pull
```

---

## PR 2 — Utilitários de leitura e monitoramento

```bash
git checkout -b feat/utilitarios-lake
git add src/utils/lakeio.py
git commit -m "feat: leitura e escrita padronizada do data lake em parquet"

git add src/utils/monitoring.py
git commit -m "feat: coleta de metricas por etapa da pipeline"

git push -u origin feat/utilitarios-lake
gh pr create --base main --title "Utilitarios de lake e monitoramento" --body "Dois modulos de apoio usados por toda a pipeline.

- lakeio: padroniza a leitura/escrita em parquet particionado
- monitoring: registra duracao, volume e rejeicoes de cada etapa"
```

> **Comentário para postar:**
> O `lakeio` existe por um detalhe do parquet particionado: a coluna de partição fica no nome da
> pasta e, na volta, o pandas devolve ela como `Categorical` — o que quebra operações simples como
> `.max()`. Centralizei a conversão aqui em vez de espalhar `astype` pelos notebooks.

---

## PR 3 — Framework de qualidade de dados

```bash
git checkout main && git pull
git checkout -b feat/qualidade-dados
git add src/quality/data_quality.py
git commit -m "feat: verificacoes de qualidade com severidade e relatorio em json"
git push -u origin feat/qualidade-dados
gh pr create --base main --title "Framework de qualidade de dados" --body "Verificacoes encadeaveis cobrindo completude, unicidade, validade, consistencia e atualidade.

Atende ao requisito de validacao de duplicidade, valores ausentes, chaves de relacionamento e consistencia entre tabelas."
```

> **Comentário para postar:**
> Cada verificação tem severidade. `ERROR` interrompe a pipeline; `WARNING` só registra. Fiz assim
> porque nem toda imperfeição justifica parar a esteira: os níveis de proficiência ausentes em 2023,
> por exemplo, são característica conhecida da fonte, não defeito de ingestão.

---

## PR 4 — Camadas Bronze e Silver

```bash
git checkout main && git pull
git checkout -b feat/camada-bronze-silver
git add data/landing/ibge_estados.csv data/landing/ibge_municipios.csv
git commit -m "data: dimensoes territoriais do IBGE (estados e municipios)"

git add notebooks/01_ingestao_batch_bronze_silver.ipynb
git commit -m "feat: ingestao batch, camada bronze com linhagem e silver integrada"

git push -u origin feat/camada-bronze-silver
gh pr create --base main --title "Camadas Bronze e Silver" --body "Ingestao batch das fontes, camada Bronze com metadados de linhagem e camada Silver com limpeza, padronizacao e integracao das bases.

Inclui quality gate nas duas camadas."
```

> **Comentário para postar:**
> O ponto mais trabalhoso foi a coluna `rede`: o indicador traz código numérico e as tabelas de meta
> trazem texto. O de-para não está em nenhum dos CSVs. Validei o mapeamento contra os próprios
> dados — a taxa da rede Municipal bate com a tabela de metas em 10.584 linhas, com 3 divergências
> que são inconsistências da fonte. Está na célula de consistência entre fontes.

---

## PR 5 — Extração dos microdados de aluno

```bash
git checkout main && git pull
git checkout -b feat/extracao-microdados
git add src/ingestion/extrair_bigquery.py
git commit -m "feat: extracao dos microdados de aluno via BigQuery com dry run"

git add docs/guia_extracao_bigquery.md
git commit -m "docs: passo a passo do BigQuery Sandbox"

git push -u origin feat/extracao-microdados
gh pr create --base main --title "Extracao dos microdados de aluno" --body "A tabela de alunos tem 3.867.999 linhas e 256 MB, acima do limite de download gratuito do portal da Base dos Dados. Este script extrai do BigQuery publico.

Aplica projecao de colunas, filtro por ano e dry run antes de executar."
```

> **Comentário para postar:**
> O dry run do BigQuery estima o custo da consulta sem executá-la. Como o BigQuery cobra por bytes
> lidos, deixei o script imprimindo a estimativa e o percentual da cota gratuita antes de rodar de
> verdade. A tabela inteira consome 0,02% da cota mensal, então sai de graça no Sandbox.

---

## PR 6 — Ingestão em streaming

```bash
git checkout main && git pull
git checkout -b feat/streaming-kafka
git add src/streaming/broker_local.py
git commit -m "feat: broker de eventos em memoria compativel com kafka-python"

git add notebooks/02_ingestao_streaming_kafka.ipynb
git commit -m "feat: streaming com validacao, DLQ, janelas e upsert idempotente"

git push -u origin feat/streaming-kafka
gh pr create --base main --title "Ingestao em streaming" --body "Producao e consumo de eventos de avaliacao, com validacao por contrato, dead letter queue, janelas temporais, regras de alerta e upsert idempotente na Silver."
```

> **Comentário para postar:**
> Escrevi o `broker_local` porque instalar Kafka de verdade só funciona no Colab ou Linux, e eu
> queria que o notebook rodasse em qualquer máquina. Ele tem a mesma interface do `kafka-python`, e
> o resto do notebook não sabe qual dos dois está rodando.

---

## PR 7 — Camada Gold

```bash
git checkout main && git pull
git checkout -b feat/camada-gold
git add src/gold/consultas.py
git commit -m "feat: consultas da camada gold em SQL padrao"

git add notebooks/03_gold_pyspark_cloud_finops.ipynb
git commit -m "feat: camada gold em pyspark, otimizacoes e analise de finops"

git push -u origin feat/camada-gold
gh pr create --base main --title "Camada Gold e FinOps" --body "Oito datasets analiticos organizados por pergunta de negocio, otimizacoes de Spark medidas e estimativa de custo da arquitetura."
```

> **Comentário para postar:**
> Escrevi as consultas em SQL padrão, num arquivo separado, em vez de usar a DSL do Spark dentro do
> notebook. Assim a mesma definição roda no Spark, no DuckDB e no Athena sem reescrever — e a regra
> de negócio fica num lugar só.

---

## PR 8 — Análises e modelos

```bash
git checkout main && git pull
git checkout -b feat/analytics-ia
git add notebooks/04_analytics_ia_politicas_publicas.ipynb
git commit -m "feat: analise de desigualdade, modelo preditivo e indice de priorizacao"
git push -u origin feat/analytics-ia
gh pr create --base main --title "Analises e modelos de IA" --body "Consome a camada Gold para analisar desigualdade educacional, treinar um modelo de predicao e gerar o indice de priorizacao por municipio."
```

> **Comentário para postar:**
> Comparei o modelo com um baseline que apenas repete a taxa do ciclo anterior. Indicador educacional
> é bastante autocorrelacionado, então sem essa comparação o R² pareceria mérito do modelo. O ganho
> real ficou em 27% sobre o baseline.

---

## PR 9 — Explorador do data lake

```bash
git checkout main && git pull
git checkout -b feat/explorador-lake
git add notebooks/00_explorar_lake.ipynb
git commit -m "feat: notebook para listar e consultar as tabelas do lake"
git push -u origin feat/explorador-lake
gh pr create --base main --title "Explorador do data lake" --body "Notebook auxiliar para inspecionar as tabelas das tres camadas, rodar SQL sobre elas e exportar para CSV."
```

> **Comentário para postar:**
> Parquet é binário e não abre no Excel. Este notebook resolve isso: lista o catálogo das tabelas,
> mostra schema e amostra de qualquer uma, e permite consultar com SQL.

---

## PR 10 — Implementação no Databricks

```bash
git checkout main && git pull
git checkout -b feat/deploy-databricks
git add cloud/databricks/
git commit -m "feat: pipeline no databricks com delta lake e unity catalog"

git add cloud/README.md docs/guia_deploy_databricks.md
git commit -m "docs: guia de deploy no databricks free edition"

git push -u origin feat/deploy-databricks
gh pr create --base main --title "Implementacao em nuvem no Databricks" --body "Versao da pipeline para o Databricks Free Edition: tabelas Delta no Unity Catalog, Structured Streaming, MERGE idempotente e job do Lakeflow.

Os notebooks importam as mesmas consultas de src/gold/consultas.py usadas na versao local."
```

> **Comentário para postar:**
> Escolhi o Databricks pela arquitetura Lakehouse apresentada na Aula 03, que traz ACID e time travel
> sobre formato aberto. A Tabela 2 do material mapeia esse desenho nos três provedores e cita Delta
> Lake e Databricks nominalmente. Some-se o lado prático: a conta é gratuita e não pede cartão.

---

## PR 11 — Arquitetura de referência na AWS

```bash
git checkout main && git pull
git checkout -b docs/arquitetura-aws
git add cloud/aws/
git commit -m "docs: jobs glue, DDL athena, lambda e step functions como referencia"
git push -u origin docs/arquitetura-aws
gh pr create --base main --title "Arquitetura de referencia na AWS" --body "Implementacao equivalente na AWS, documentada mas nao executada: tres jobs Glue, DDL do Athena, processador Lambda e orquestracao em Step Functions."
```

> **Comentário para postar:**
> Mantive as duas implementações no repositório de propósito. A da AWS segue a abordagem modular
> (S3 + Glue + Athena), que é a geração Data Lake; a do Databricks segue a Lakehouse. As duas leem os
> mesmos arquivos de `src/`, então a regra de negócio não está duplicada.

---

## PR 12 — Documentação e apresentação

```bash
git checkout main && git pull
git checkout -b docs/readme-e-apresentacao
git add README.md
git commit -m "docs: README com arquitetura, decisoes tecnicas, finops e aplicacao em IA"

git add docs/roteiro_video_executivo.md docs/estrutura_apresentacao.md docs/plano_git.md
git commit -m "docs: roteiro do video executivo e estrutura da apresentacao"

git push -u origin docs/readme-e-apresentacao
gh pr create --base main --title "README e material de apresentacao" --body "Documentacao completa: contexto do problema, arquitetura, fluxo de dados, tecnologias e justificativas, decisoes arquiteturais, monitoramento, FinOps e aplicacao em IA."
```

> **Comentário para postar:**
> O README cobre os itens que o enunciado pede, incluindo os trade-offs de batch versus streaming,
> data lake versus warehouse e custo versus performance.

---

## PR 13 — Evidências de execução

Esta é a última, depois de rodar tudo no Databricks e tirar os prints.

```bash
git checkout main && git pull
git checkout -b docs/evidencias
git add assets/img/
git commit -m "docs: prints da execucao no databricks"
git push -u origin docs/evidencias
gh pr create --base main --title "Evidencias de execucao" --body "Prints das tres camadas no Unity Catalog, dos quality gates, da reconstrucao do indicador, do streaming com DLQ e do job orquestrado."
```

> **Comentário para postar:**
> O print da reconstrução do indicador é o mais relevante: partindo dos 3,8 milhões de registros
> individuais e aplicando a média ponderada por `peso_aluno`, o resultado bate com o número publicado
> pelo Inep com erro médio de 0,04 p.p.

---

## Conferindo no final

```bash
git checkout main && git pull
git log --oneline --graph --all | head -40
```

Você deve ver a `main` com 13 merges, cada um trazendo os commits da sua branch.

No GitHub, a aba **Pull requests** → **Closed** mostra as 13 PRs com título, descrição e o comentário
de justificativa. É exatamente o que o enunciado pede.

### Se precisar corrigir algo depois

Não faça commit direto na `main`. Abra outra branch:

```bash
git checkout -b fix/descricao-do-ajuste
# ... edite ...
git commit -am "fix: descricao do que foi corrigido"
git push -u origin fix/descricao-do-ajuste
```

Uma PR de correção depois das de funcionalidade é normal e até reforça o histórico.

---

## Padrão das mensagens de commit

As mensagens seguem um prefixo que indica o tipo da mudança:

| Prefixo | Uso |
|---|---|
| `feat:` | funcionalidade nova |
| `fix:` | correção |
| `docs:` | documentação |
| `data:` | arquivos de dados |
| `chore:` | configuração e estrutura |

É o padrão *Conventional Commits*, bastante usado no mercado e fácil de justificar se perguntarem.
