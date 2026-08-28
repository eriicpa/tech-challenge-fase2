# Implementações em nuvem

Este diretório tem **duas implementações da mesma arquitetura**. Elas não se misturam: nenhum
arquivo de uma importa nada da outra.

| Pasta | O que é | Status |
|---|---|---|
| `databricks/` | Implementação **executada**. Delta Lake, Unity Catalog, Structured Streaming e job do Lakeflow. | ✅ rodando |
| `aws/` | Arquitetura de **referência**. Jobs Glue, DDL do Athena, Lambda de streaming e Step Functions. | 📄 documentada, não executada |

## Por que as duas

A Aula 03 apresenta o mesmo desenho arquitetural mapeado nos três provedores (Tabela 2), e trata
Lakehouse, Data Lake e Data Warehouse como gerações de uma mesma evolução. As duas pastas
materializam esse ponto:

- A versão **AWS** segue a abordagem modular — serviços especializados combinados (S3 + Glue +
  Athena), com o dado em Parquet sobre object storage. É a geração **Data Lake**.
- A versão **Databricks** segue a abordagem Lakehouse — formatos abertos com camada transacional,
  o que traz ACID, *time travel* e `MERGE` que o Parquet puro não oferece. É a geração **Lakehouse**.

A escolha pelo Databricks para execução é justificada no README principal, na seção de decisões
arquiteturais.

## O que é compartilhado

A regra de negócio não está duplicada. As duas implementações leem os mesmos arquivos de `src/`:

```
src/gold/consultas.py       as 8 consultas da camada Gold, em SQL padrão
src/quality/data_quality.py o framework de verificações
src/utils/monitoring.py     as métricas de execução
```

O notebook `databricks/03_gold.py` e o job `aws/glue/etl_gold.py` importam **o mesmo**
`src/gold/consultas.py`. Se a definição de "risco crítico" mudar, muda em um lugar só.
