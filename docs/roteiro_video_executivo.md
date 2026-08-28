# Roteiro do Vídeo Executivo — até 5 minutos

**Tech Challenge Fase 2 — Pipeline Híbrido para Análise da Alfabetização no Brasil**

---

## Conformidade com o enunciado

O PDF exige que o vídeo aborde quatro tópicos. O roteiro está organizado exatamente neles, para que
o avaliador consiga marcar cada item:

| Exigência do PDF | Bloco | Tempo |
|---|---|---|
| Problema de negócio | 1 | 0:10 – 1:15 |
| Arquitetura da solução | 2 | 1:15 – 2:30 |
| Valor da pipeline para análises educacionais | 3 | 2:30 – 3:45 |
| Potencial uso para inteligência artificial | 4 | 3:45 – 4:35 |

As outras três exigências: **um integrante apresenta** (bloco 0), **linguagem executiva** e
**simulação de apresentação para liderança** (o roteiro inteiro fala com um secretário de educação,
não com uma banca técnica).

**Ritmo:** o texto tem **684 palavras faladas**. A 145 palavras por minuto — ritmo de apresentação
normal — fecha em **4min43s**. Mas se você falar mais devagar, a 130 por minuto, passa para
**5min16s** e **estoura o limite**.

Ou seja: cronometre a sua própria leitura antes de gravar. Se a sua leitura seca passar de 4min40s,
aplique os cortes da seção "Se estourar o tempo" — eles foram escolhidos justamente para caber sem
derrubar nenhum dos quatro tópicos exigidos.

**Regra de ouro:** nada de código na tela. Mostre resultado, gráfico ou diagrama.

---

## Bloco 0 — Abertura `0:00 – 0:10`

**Tela:** capa com título, nomes do grupo e data.

> "Olá. Sou [NOME], do grupo [GRUPO], e vou apresentar a pipeline de dados que construímos para
> apoiar o Compromisso Nacional Criança Alfabetizada."

---

## Bloco 1 — Problema de negócio `0:10 – 1:15`

**Tela:** o número **80%** grande, e ao lado **63%**. Depois, o boxplot por região.

> "O Brasil assumiu um compromisso: **até 2030, toda criança alfabetizada até o fim do segundo ano**.
> O Inep definiu o que conta como alfabetizado — 743 pontos na escala do Saeb — e hoje a rede
> municipal está em **63%**.
>
> Só que essa média nacional esconde o problema real. A região Centro-Oeste está em **72%**; a região
> Norte, em **49%**. E dentro de um mesmo estado existem municípios com 90% e municípios com 20%.
>
> Quem coordena essa política precisa responder uma pergunta simples: **onde investir primeiro?**
>
> E não conseguia responder rápido. A informação existe, mas espalhada em seis bases diferentes — o
> indicador por município, as metas nacionais, estaduais e municipais, os dados de território e os
> microdados de alunos. Formatos diferentes, códigos diferentes para a mesma coisa, sem chave em
> comum. Cada análise virava um projeto de semanas.
>
> E quando o dado chega tarde, ele já não muda a decisão. Enquanto isso, a criança passa para o
> terceiro ano sem saber ler."

---

## Bloco 2 — Arquitetura da solução `1:15 – 2:30`

**Tela:** diagrama da arquitetura medalhão, com as duas entradas (batch e streaming) convergindo.

> "A solução é uma **pipeline híbrida de dados, rodando em nuvem**, organizada em três camadas.
>
> A camada **Bronze** guarda o dado exatamente como veio da fonte, com rastreabilidade completa:
> sabemos de onde veio cada linha e quando entrou. Nada se perde, e qualquer erro é corrigível por
> reprocessamento.
>
> A camada **Silver** limpa, padroniza e — este é o ponto central — **integra as seis fontes em uma
> visão única do município**. É aqui que o trabalho de engenharia acontece: harmonizar códigos,
> validar chaves, resolver as incompatibilidades entre as bases.
>
> A camada **Gold** entrega os dados prontos para decisão: painel, análise e modelos.
>
> Para dar escala ao número: a pipeline integra **3,9 milhões de registros individuais de alunos**
> aos indicadores de 5.500 municípios — e reconstrói, a partir desses microdados, exatamente o mesmo
> indicador que o Inep publica.
>
> Ela é **híbrida** porque combina dois ritmos. **Processamento em lote** para o histórico
> consolidado. E **streaming**, em tempo quase real, durante as semanas em que a avaliação está sendo
> aplicada nas escolas — quando saber hoje, e não no ano que vem, muda o que se faz.
>
> Duas garantias fazem parte do desenho. **Qualidade validada automaticamente** em cada camada: se o
> dado não passa, a esteira para. E **custo controlado**: o streaming só fica ligado na janela da
> avaliação, o que sozinho economiza cerca de **cinco mil e oitocentos dólares por ano**."

---

## Bloco 3 — Valor da pipeline para análises educacionais `2:30 – 3:45`

**Tela:** primeiro o gráfico da variação por UF, com o estado anômalo em vermelho. Depois, a tabela
de comparação meta × realizado.

> "O valor disso aparece de duas formas.
>
> A primeira é velocidade: perguntas que levavam semanas passaram a ser respondidas em segundos,
> sobre uma base única e confiável.
>
> A segunda é mais importante, e vou mostrar com um caso real deste projeto. A validação automática
> acendeu um alerta: **um estado inteiro aparecia com queda de 18 pontos percentuais, enquanto o país
> subia 2**. Investigamos, e o padrão era de mudança de metodologia na origem — não de perda de
> aprendizagem.
>
> Se essa verificação não existisse, o nosso ranking apontaria aquele estado como a maior emergência
> educacional do país, e o investimento seria direcionado para o lugar errado. **A pipeline impediu
> uma decisão errada de milhões de reais.**
>
> Com a base confiável, a análise responde o que a gestão precisa: quanto cada município está longe
> da própria meta, quem melhorou e quem piorou entre os ciclos, e quantos chegam a 2030 no ritmo
> atual. A resposta hoje é dura: **quase metade dos municípios não chega**, se nada mudar."

---

## Bloco 4 — Potencial uso para inteligência artificial `3:45 – 4:35`

**Tela:** três colunas — Prever / Agrupar / Priorizar — e depois a tabela dos municípios críticos.

> "Como a camada Gold já sai pronta e confiável, ela vira insumo direto para inteligência artificial.
> Aplicamos em três frentes.
>
> **Prever.** Um modelo estima a taxa do próximo ciclo com erro médio de 9 pontos percentuais, quase
> **30% melhor** do que simplesmente repetir o resultado do ano anterior. Isso permite agir *antes*
> da próxima avaliação, e não depois.
>
> **Agrupar.** Os municípios foram segmentados em três perfis — vulnerabilidade crítica, atenção
> prioritária e em rota de melhoria. Cada perfil pede uma política diferente.
>
> **Priorizar.** A pipeline entrega uma lista ordenada de **401 municípios em situação crítica**, com
> nome, estado e o tamanho exato do esforço anual necessário para alcançar a meta.
>
> Deixou de ser um relatório: virou uma **lista de para onde ir na segunda-feira de manhã**. E o
> caminho para produção já está desenhado — retreino automático a cada novo ciclo e monitoramento
> para detectar quando o modelo perder validade."

---

## Bloco 5 — Fechamento `4:35 – 4:50`

**Tela:** arquitetura em miniatura com as próximas fontes chegando pontilhadas.

> "Integramos seis fontes públicas em uma base confiável, com qualidade verificada, monitoramento e
> custo controlado. Ela já está preparada para receber Censo Escolar, dados socioeconômicos do IBGE e
> investimento do FUNDEB — o que vai permitir responder não só *onde* está o problema, mas *por quê*.
>
> Obrigado."

---

## Se estourar o tempo

Corte nesta ordem, e só até caber:

1. No bloco 3, a frase sobre velocidade (as duas primeiras falas) — o caso do estado anômalo sozinho já sustenta o bloco.
2. No bloco 2, a frase sobre FinOps — é requisito de README, não de vídeo.
3. No bloco 4, a frente "Agrupar".

**Nunca corte:** o bloco 1 inteiro, a explicação das três camadas no bloco 2, o caso do estado
anômalo no bloco 3 e a lista dos 401 municípios no bloco 4. São esses trechos que cobrem
literalmente os quatro tópicos exigidos.

## Checklist de gravação

| Item | Detalhe |
|---|---|
| Duração | Cronometre a leitura seca. Se passar de 4:55, corte na ordem acima |
| Apresentador | Pelo menos um integrante fala e aparece — apareça na abertura e no fechamento |
| Linguagem | Executiva: frases curtas, números redondos, zero jargão não explicado |
| Não dizer | "parquet", "particionamento", "broadcast join", "DPU", "commit de offset", "medalhão" |
| Pode dizer | "camada", "pipeline", "streaming", "modelo preditivo" — todos explicados no próprio texto |
| Tela | Resultado, gráfico e diagrama. Nunca código, terminal ou célula de notebook |
| Gravação | Tela + voz; câmera no canto na abertura e no fechamento |

## Ensaio (30 minutos)

1. Leia em voz alta cronometrando, e anote onde tropeçar.
2. Reescreva **com suas palavras** os trechos onde tropeçou — texto decorado soa artificial.
3. Grave uma vez sem se preocupar com erro, só para calibrar o ritmo.
4. Grave a versão final com as telas sincronizadas.
