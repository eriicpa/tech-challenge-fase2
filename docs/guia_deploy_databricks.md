# Passo a passo — rodar a pipeline no Databricks

Documento operacional: do zero até tudo rodando, com o que esperar em cada etapa e o que fazer se der
errado. Ao final, as três camadas estarão no Unity Catalog, o job orquestrado, e os prints prontos
para a apresentação.

**Tempo:** 60 a 90 minutos na primeira vez · **Custo:** zero · **Cartão de crédito:** não é pedido

---

## Antes de começar

| Item | Como conferir |
|---|---|
| Conta no GitHub | o repositório precisa estar publicado antes do passo 3 |
| `data/landing/alunos.parquet` (53 MB) | é o único arquivo que não vai para o Git |
| Os outros 7 arquivos em `data/landing/` | 5 CSVs do Inep e 2 CSVs do IBGE |
| Um e-mail para criar a conta | pode ser pessoal |

Se `alunos.parquet` não existir na sua máquina, gere com o script de extração:

```bash
python -m src.ingestion.extrair_bigquery --projeto SEU-PROJETO-GCP
```

O passo a passo do BigQuery Sandbox (gratuito, sem cartão) está em `docs/guia_extracao_bigquery.md`.

---

## Etapa 1 — Publicar o repositório no GitHub

O Databricks vai clonar o repositório, então ele precisa existir lá antes. Se você for seguir o
plano de commits (`docs/plano_git.md`), faça isso primeiro e volte aqui.

```bash
git remote -v
```

Se não retornar nada, o repositório ainda não foi publicado. Nesse caso:

```bash
git init
git add .
git commit -m "estrutura inicial do projeto"
git branch -M main
git remote add origin https://github.com/SEU-USUARIO/SEU-REPO.git
git push -u origin main
```

Confira depois do push que `alunos.parquet` **não** subiu — ele está no `.gitignore` por causa do
tamanho, e é enviado direto ao Databricks na Etapa 5.

---

## Etapa 2 — Criar a conta no Databricks

1. Acesse <https://www.databricks.com/learn/free-edition>
2. Clique em **Get started free**
3. Preencha e confirme o e-mail
4. Escolha **Free Edition**

> Cuidado para não cair na avaliação de 14 dias (*14-day free trial*), que pede cartão. A opção
> correta é **Free Edition**, gratuita por tempo indeterminado.

Ao entrar, o workspace já vem com Unity Catalog e compute serverless configurados. Não é preciso
criar cluster.

---

## Etapa 3 — Importar o repositório

1. Menu lateral esquerdo: **Workspace**
2. Botão **Create**, no canto superior direito
3. Escolha **Git folder**
4. Em *Git repository URL*, cole `https://github.com/SEU-USUARIO/SEU-REPO.git`
5. O campo *Git provider* preenche sozinho como GitHub
6. Clique em **Create Git folder**

**Resultado esperado:** a pasta aparece em `Workspace / Users / seu-email / SEU-REPO`, com as
subpastas `notebooks`, `cloud`, `src` e `docs`.

Se o repositório for privado, o Databricks vai pedir autenticação. Vá em **Settings** → **Linked
accounts** → **Git integration**, escolha GitHub e cole um Personal Access Token (no GitHub:
*Settings → Developer settings → Personal access tokens → Tokens (classic)*, com o escopo `repo`).

---

## Etapa 4 — Criar os schemas e o volume

1. Na Git folder, abra `cloud` → `databricks` → `01_bronze_silver`
2. No canto superior direito, clique em **Connect** e escolha **Serverless**
3. Aguarde o indicador ficar verde (leva 1 a 2 minutos na primeira vez)
4. Clique na **primeira célula de código** (a que começa com `# PARÂMETROS DO JOB`) e rode só ela,
   com `Shift + Enter`

**Resultado esperado:**

```
Volume de landing: /Volumes/workspace/tc2_landing/arquivos
Run: 20260827_...
```

Isso confirma que foram criados os schemas `tc2_landing`, `tc2_bronze`, `tc2_silver` e o volume
`arquivos`. Você vai ver também os campos de parâmetro aparecerem no topo do notebook — são os
widgets, equivalentes ao *Job parameters* do AWS Glue.

---

## Etapa 5 — Enviar os arquivos

1. Menu lateral: **Catalog**
2. Navegue: `workspace` → `tc2_landing` → `arquivos`
3. Botão **Upload to this volume**
4. Arraste os **8 arquivos** da pasta `data/landing/` do seu computador:

```
br_inep_avaliacao_alfabetizacao_municipio.csv
br_inep_avaliacao_alfabetizacao_uf.csv
br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv
br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv
br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv
ibge_estados.csv
ibge_municipios.csv
alunos.parquet
```

5. **Upload**

O `alunos.parquet` tem 53 MB e é o que demora — de 2 a 5 minutos, dependendo da sua conexão. Os
outros são instantâneos. O limite da plataforma é 5 GB por arquivo, então não há risco.

**Resultado esperado:** os 8 arquivos listados na tela do volume.

---

## Etapa 6 — Notebook 01: Bronze e Silver

Volte ao notebook `01_bronze_silver` e clique em **Run all**, no topo.

**Duração:** 6 a 12 minutos.

### O que conferir, célula por célula

| Célula | Saída esperada |
|---|---|
| Parâmetros | `Volume de landing: /Volumes/workspace/tc2_landing/arquivos` |
| Conferência dos arquivos | `Os 8 arquivos estão no volume.` e a listagem |
| Ingestão Bronze | 8 linhas `[BRONZE] ...`, sendo `aluno` com **3.867.999** linhas |
| Quality gate Bronze | tabela de verificações, todas `PASS`, e `todas aprovadas.` no final |
| Dimensões territoriais | `dim_uf: 27 \| dim_municipio: 5570` |
| Fatos do indicador | `indicador municipio: 23995 \| indicador uf: 145` |
| Silver de aluno | `Efetivamente avaliados : 3.354.661 (86.7%)` |
| **Reconstrução do indicador** | `Erro medio na taxa : 0.0376 p.p.` e `Reconstrucao aprovada` |
| Trajetórias de meta | `Trajetorias de meta e participacao gravadas.` |
| Tabela integrada | amostra da `fato_alfabetizacao_municipio` |
| Consistência entre fontes | `Mapeamento rede=3 -> Municipal confirmado pelos dados` e 3 divergências |
| Resumo | 11 tabelas na Silver |

A célula da **reconstrução** é a mais importante do trabalho: ela mostra que a pipeline chega ao
mesmo número que o Inep publica, partindo dos 3,8 milhões de registros individuais. É o print
número 3 da lista da Etapa 11.

---

## Etapa 7 — Notebook 02: Streaming

Abra `cloud/databricks/02_streaming` e clique em **Run all**.

**Duração:** 3 a 5 minutos.

### O que conferir

| Célula | Saída esperada |
|---|---|
| Parâmetros | caminhos de eventos e checkpoint |
| Geração de eventos | `Registros reais no fluxo: 1200` e três lotes publicados |
| Stream processor | `Stream encerrado.` |
| Resultado da ingestão | válidos em torno de 95%, DLQ em torno de 5% |
| Motivos de rejeição | tabela cruzando o defeito injetado com o motivo detectado |
| Janelas | janelas de 10 segundos por UF, com latência p95 |
| Alertas | quantidade maior que zero, com os municípios listados |
| MERGE | `Apos reaplicar o mesmo lote: N linhas (esperado: N, inalterado)` |
| Observabilidade | latência média e p95, taxa de DLQ |
| Histórico Delta | tabela do `DESCRIBE HISTORY` com as versões |

Na tabela de motivos de rejeição, cada defeito injetado tem que aparecer com o motivo correspondente
detectado. Se `campo_faltante` aparecer como `campos_obrigatorios_ausentes`, a validação está certa.

> **Sobre o streaming no serverless:** só o gatilho `availableNow` é suportado — o notebook já usa
> esse. Ele processa tudo o que chegou e encerra, em vez de ficar ligado esperando evento. Além de
> ser o único suportado, é a decisão certa de custo.

---

## Etapa 8 — Notebook 03: Gold

Abra `cloud/databricks/03_gold` e clique em **Run all**.

**Duração:** 3 a 5 minutos.

### O que conferir

| Célula | Saída esperada |
|---|---|
| Import das consultas | `Consultas carregadas: 8` e a lista com as perguntas de negócio |
| Views da Silver | 9 linhas `silver_... -> workspace.tc2_silver...` |
| Execução | 8 linhas `[GOLD] ...` com contagem e tempo |
| Quality gate | todas as verificações `PASS` |
| Aviso de série histórica | `[AVISO] Possivel quebra de serie historica em: RS` |
| OPTIMIZE | 3 linhas de `OPTIMIZE ... ZORDER BY` |
| Consultas `%sql` | quatro tabelas de resultado |
| Inventário final | todas as tabelas das três camadas |

O aviso sobre o RS **é esperado e não é erro**. É a pipeline detectando que aquele estado se move de
forma incompatível com o resto do país entre 2023 e 2024, o que indica mudança de metodologia na
origem. Vale print — é um dos pontos fortes da apresentação.

Se o `OPTIMIZE` retornar indisponível, siga em frente: o notebook trata isso e continua.

---

## Etapa 9 — Criar o job

Rodar notebook a notebook prova que funciona. Um job prova que está orquestrado, que é o que o
enunciado pede.

1. Menu lateral: **Jobs & Pipelines**
2. **Create** → **Job**
3. Nome do job, no topo: `tc2-alfabetizacao-pipeline`
4. Configure a primeira tarefa:
   - *Task name*: `bronze_silver`
   - *Type*: Notebook
   - *Source*: Workspace
   - *Path*: navegue até `.../SEU-REPO/cloud/databricks/01_bronze_silver`
   - *Compute*: Serverless
5. **Add task** → repita para as outras duas:

| Task name | Path | Depends on |
|---|---|---|
| `streaming` | `.../cloud/databricks/02_streaming` | `bronze_silver` |
| `gold` | `.../cloud/databricks/03_gold` | `streaming` |

6. No painel direito do job (não da tarefa), procure **Tags** e adicione:

```
Environment = dev
Layer       = medallion
ManagedBy   = databricks
Owner       = seu-nome
Pipeline    = tc2-alfabetizacao
```

7. Em **Schedule**, deixe em *Manual*. Agendar consumiria quota sem necessidade, e o dado de origem é
   anual.
8. **Run now**

**Resultado esperado:** o diagrama mostra as três tarefas ligadas em sequência, e as três ficam
verdes ao final. Esse print comprova a orquestração.

---

## Etapa 10 — Dashboard

1. Menu lateral: **SQL Editor**
2. Cole a consulta abaixo e clique em **Run**:

```sql
SELECT nome_regiao,
       COUNT(*) AS municipios,
       ROUND(AVG(taxa_alfabetizacao), 2) AS taxa_media,
       ROUND(AVG(ritmo_anual_necessario_pp), 2) AS ritmo_necessario
FROM workspace.tc2_gold.gold_indicador_municipio
WHERE ano = 2024 AND rede_nome = 'Municipal'
GROUP BY nome_regiao
ORDER BY taxa_media DESC;
```

3. No resultado, clique no **+** ao lado da aba *Result* → **Visualization**
4. Escolha o tipo **Bar**, com `nome_regiao` no eixo X e `taxa_media` no Y
5. **Save**
6. Clique nos três pontos da visualização → **Add to dashboard** → crie um dashboard novo

Repita com as outras consultas do notebook 03. Três gráficos já formam um painel apresentável, e
comprovam o requisito de "camada Gold preparada para dashboards".

---

## Etapa 11 — Prints para a apresentação

| # | Print | Onde | Slide |
|---|---|---|---|
| 1 | Catalog com os 3 schemas e as tabelas | Catalog → workspace | A7 |
| 2 | Quality gate da Bronze aprovado | notebook 01 | A3 |
| 3 | Reconstrução do indicador | notebook 01 | A2b |
| 4 | Cruzamento indicador × metas | notebook 01 | A3 |
| 5 | DLQ com os motivos de rejeição | notebook 02 | A5 |
| 6 | Janelas e alertas | notebook 02 | A5 |
| 7 | Idempotência do MERGE | notebook 02 | A5 |
| 8 | Inventário da Gold | notebook 03 | A7 |
| 9 | As 4 consultas de negócio | notebook 03 | 9 e 10 |
| 10 | Job com as 3 tarefas verdes e as tags | Jobs & Pipelines | A4 |
| 11 | `DESCRIBE HISTORY` | notebook 02 | A7 |
| 12 | Dashboard | SQL → Dashboards | 9 |

Salve tudo em `assets/img/` do repositório e faça commit. Os números de cada slide estão em
`docs/estrutura_apresentacao.md`.

---

## Se der errado

| Mensagem | Causa | O que fazer |
|---|---|---|
| `Faltam N arquivo(s) no volume` | upload incompleto | Volte à Etapa 5 e confira os 8 nomes, inclusive a extensão |
| `Nao encontrei src/gold/consultas.py` | notebook aberto fora da Git folder | Abra pelo caminho `Workspace/Users/.../SEU-REPO/cloud/databricks/...` |
| `PERMISSION_DENIED` ao criar schema | catálogo diferente do padrão | Ajuste o parâmetro `catalogo` no topo do notebook |
| `quota exceeded` / compute não sobe | limite diário da Free Edition | Espere o dia seguinte. A pipeline é idempotente e pode rodar de novo do zero |
| Notebook 02 falha em `readStream` | checkpoint de execução anterior | A primeira célula já limpa o checkpoint; rode o notebook desde o início |
| `OPTIMIZE ... indisponivel` | recurso restrito no workspace | É apenas aviso; o notebook continua |
| Upload do parquet muito lento | 53 MB | Deixe rodando em outra aba e siga com os CSVs |
| Job falha mas o notebook funciona | *Path* da tarefa errado | Confira o caminho de cada tarefa na Etapa 9 |
| `Tabela nao encontrada: tc2_silver...` | notebooks fora de ordem | A ordem é 01 → 02 → 03 |

---

## Checklist antes de apresentar

- [ ] Os 3 notebooks rodaram do início ao fim sem erro
- [ ] O job rodou com as 3 tarefas verdes
- [ ] Os 12 prints estão salvos em `assets/img/`
- [ ] O repositório no GitHub está atualizado, incluindo os prints
- [ ] O README aponta para as evidências
- [ ] O roteiro do vídeo foi cronometrado (`docs/roteiro_video_executivo.md`)
- [ ] Você consegue explicar, com suas palavras, o que é a reconstrução do indicador e por que a
      média é ponderada

O último item costuma ser a pergunta da banca. A resposta está na célula de reconstrução do
notebook 01: cada aluno tem um peso amostral, então a taxa oficial é média ponderada; usando média
simples o número dá 59,16% em vez dos 58,41% corretos.
