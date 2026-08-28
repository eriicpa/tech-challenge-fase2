# Estrutura da Apresentação (PPT)

**Tech Challenge Fase 2 — Pipeline Híbrido para Análise da Alfabetização no Brasil**

Estrutura pronta para uma ferramenta de design. Cada slide traz a **mensagem única** (a frase que a
pessoa deve levar), o conteúdo, o visual sugerido e o **bloco do roteiro** correspondente — assim o
deck e o vídeo ficam sincronizados.

Os slides seguem a mesma divisão exigida pelo PDF para o vídeo: problema de negócio, arquitetura da
solução, valor para análises educacionais e potencial uso para IA.

> **Regra de design:** um slide, uma ideia. Números grandes, texto mínimo. O que está no slide não é
> o que você fala — é o que reforça o que você fala.

---

## Parte 1 — Deck do vídeo executivo (12 slides · 5 minutos)

### Slide 1 — Capa · `bloco 0`
| | |
|---|---|
| **Mensagem** | Este é um projeto sobre alfabetização, não sobre tecnologia |
| **Conteúdo** | *Pipeline Híbrido para Análise da Alfabetização no Brasil* · *Tech Challenge — Fase 2* · nomes do grupo · data |
| **Visual** | Imagem de sala de aula com overlay escuro e texto branco |

---

### 🔹 Problema de negócio — slides 2 a 4 · `bloco 1`

### Slide 2 — O compromisso e a distância até ele
| | |
|---|---|
| **Mensagem** | A meta é clara; a distância até ela, não |
| **Conteúdo** | **80%** meta 2030 · **63%** taxa atual da rede municipal · **743 pontos** o corte oficial do Saeb |
| **Visual** | Três números gigantes lado a lado, com barra de progresso da meta |

### Slide 3 — A média esconde o problema
| | |
|---|---|
| **Mensagem** | A desigualdade está *dentro* da média |
| **Conteúdo** | Centro-Oeste **72%** vs. Norte **49%** · municípios de 90% e de 20% no mesmo estado |
| **Visual** | Boxplot por região (NB04, Etapa 2.1) |

### Slide 4 — Por que era difícil responder
| | |
|---|---|
| **Mensagem** | O problema não era falta de dado — era dado desconectado |
| **Conteúdo** | 6 fontes · formatos diferentes · códigos diferentes para a mesma coisa · sem chave comum · cada análise levava semanas |
| **Visual** | Seis ícones soltos à esquerda → seta → uma tabela única à direita |

---

### 🔹 Arquitetura da solução — slides 5 a 7 · `bloco 2`

### Slide 5 — Três camadas
| | |
|---|---|
| **Mensagem** | Bronze guarda, Silver integra, Gold entrega |
| **Conteúdo** | Bronze = bruto rastreável · Silver = limpo e **integrado** · Gold = pronto para decisão |
| **Visual** | Diagrama medalhão horizontal com as duas entradas convergindo |

### Slide 6 — Híbrida: lote e tempo real
| | |
|---|---|
| **Mensagem** | Cada dado no ritmo que ele exige |
| **Conteúdo** | **Lote:** histórico consolidado · **Streaming:** durante as semanas de aplicação da prova, quando saber hoje muda o que se faz |
| **Visual** | Linha do tempo anual com a janela de streaming destacada em cor quente |

### Slide 7 — Qualidade e custo fazem parte do desenho
| | |
|---|---|
| **Mensagem** | Se o dado não passa, a esteira para |
| **Conteúdo** | Validação automática em cada camada · **US$ 5.814/ano** economizados ao ligar o streaming só na janela |
| **Visual** | Ícone de gate nas três camadas + cartão com o número da economia |

---

### 🔹 Valor para análises educacionais — slides 8 e 9 · `bloco 3`

### Slide 8 — ⭐ O alerta que mudou a conclusão
| | |
|---|---|
| **Mensagem** | Governança de dados evita decisão errada de milhões |
| **Conteúdo** | Um estado com **−18 p.p.** enquanto o país sobe **+2 p.p.** · causa: mudança de metodologia na origem · resultado: ranking de prioridade totalmente diferente |
| **Visual** | Barras da variação por UF com a UF anômala em vermelho (NB04, Etapa 3.3) |
| | **Slide mais importante do deck — ensaie especialmente este** |

### Slide 9 — O que a gestão passa a responder
| | |
|---|---|
| **Mensagem** | Perguntas de semanas viraram perguntas de segundos |
| **Conteúdo** | Distância da própria meta por município · quem melhorou e quem piorou · **quase metade não chega a 2030** no ritmo atual |
| **Visual** | Tabela meta × realizado com 5 linhas reais + um número grande de destaque |

---

### 🔹 Potencial uso para IA — slides 10 e 11 · `bloco 4`

### Slide 10 — Prever, agrupar, priorizar
| | |
|---|---|
| **Mensagem** | A Gold vira decisão por três caminhos |
| **Conteúdo** | **Prever:** erro de 9 p.p., ~30% melhor que a regra ingênua · **Agrupar:** 3 perfis de município · **Priorizar:** índice por município |
| **Visual** | Três colunas com ícone, título e uma métrica cada |

### Slide 11 — A entrega concreta
| | |
|---|---|
| **Mensagem** | Não é um relatório: é uma lista de ação |
| **Conteúdo** | **401 municípios** em prioridade crítica, com estado, gap até a meta e esforço anual necessário |
| **Visual** | Tabela com 6–8 linhas reais da `gold_priorizacao_municipio`, fonte grande, poucas colunas |

---

### Slide 12 — Fechamento · `bloco 5`
| | |
|---|---|
| **Mensagem** | A base está pronta para crescer |
| **Conteúdo** | Próximas fontes: Censo Escolar · IBGE/PNAD · FUNDEB · *"responder não só onde está o problema, mas por quê"* |
| **Visual** | Arquitetura em miniatura com as novas fontes chegando pontilhadas |

---

## Parte 2 — Slides de apoio (banca técnica)

Não entram no vídeo de 5 minutos. Deixe-os **depois do slide final**, prontos para serem chamados se
alguém perguntar.

| # | Slide | Conteúdo |
|---|---|---|
| A1 | Modelo de dados da Silver | Esquema estrela: dimensões, fatos e a tabela integrada |
| A2 | As 6 fontes e seus grãos | Catálogo com origem, modo de ingestão, grão e chave — todas as 6 com dado real |
| A2b | **Reconstrução do indicador** | Partindo de 3,9 mi de microdados, a média ponderada reproduz o indicador oficial com erro de 0,04 p.p. |
| A3 | Regras de qualidade | As 9 verificações, dimensões cobertas e o conceito de quality gate |
| A4 | Evidência de execução | Painel: etapas, duração, linhas, rejeições e alertas |
| A5 | Streaming em detalhe | Tópicos, DLQ, janelas e o painel com lag, latência p95 e taxa de DLQ |
| A6 | FinOps — as decisões | Tabela decisão × alternativa descartada × efeito, e a estimativa de custo |
| A7 | Arquitetura de nuvem | Databricks (implementado) e AWS (arquitetura de referência) lado a lado |
| A8 | Trade-offs assumidos | Batch vs. streaming · Data lake vs. warehouse · Custo vs. performance · Spark vs. pandas |
| A9 | Resultado do modelo | Previsto vs. observado, resíduos e importância das variáveis |
| A10 | Limitações declaradas | 2 ciclos de dados · microdados de aluno simulados · correlação ≠ causalidade |

---

## Imagens a exportar dos notebooks

Gere uma vez e salve em `assets/img/` para reutilizar no deck e no README:

| Arquivo | Origem | Slide |
|---|---|---|
| `distribuicao_taxa.png` | NB04, Etapa 2.1 (histograma + boxplot) | 3 |
| `arquitetura_medalhao.png` | diagrama próprio, feito no design | 5 e 12 |
| `custo_streaming.png` | NB03, Etapa 5.3 (estimativa de custo) | 7 |
| `quebra_serie_uf.png` | NB04, Etapa 3.3 (barras por UF) | 8 ⭐ |
| `meta_vs_realizado.png` | NB04, Etapa 2.2 | 9 |
| `clusters_municipios.png` | NB04, Etapa 4.1 (dispersão dos perfis) | 10 |
| `bytes_por_estrategia.png` | NB03, Etapa 5.2 | A6 |
| `painel_execucao.png` | NB01, Etapa 5.1 (print do painel) | A4 |
| `painel_streaming.png` | NB02, Etapa 6.1 (print do painel) | A5 |

> Para salvar um gráfico em vez de só exibi-lo, troque `plt.show()` por
> `plt.savefig("assets/img/nome.png", dpi=150, bbox_inches="tight")` antes do `show`.

---

## Paleta e tipografia

| Uso | Cor |
|---|---|
| Primária (confiança, dado) | `#2E5EAA` azul |
| Alerta / problema | `#C1272D` vermelho |
| Positivo / meta atingida | `#2E7D32` verde |
| Destaque / atenção | `#F2A104` âmbar |
| Texto | `#1A1A1A` sobre fundo `#FAFAFA` |

São as mesmas cores dos gráficos dos notebooks, então o deck e as imagens ficam coerentes sem
retrabalho.

**Tipografia:** uma família só (Inter, Source Sans ou Roboto). Título 40–48pt, número de destaque
72–96pt, corpo 20–24pt. Nada abaixo de 18pt.
