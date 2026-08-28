-- ============================================================================
-- Athena — Camada GOLD da pipeline de alfabetização
-- ============================================================================
-- Como usar:
--   1. Rode o Glue Crawler sobre s3://<conta>-tc2-data-spec/gold/ (recomendado:
--      ele infere o schema e registra as partições automaticamente), OU
--   2. Execute os DDLs abaixo e, depois, MSCK REPAIR TABLE para carregar as partições.
--
-- FinOps: todas as consultas de exemplo filtram por `ano` (coluna de partição) e
-- projetam apenas as colunas necessárias. Cobrança do Athena = bytes escaneados.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS tc2_alfabetizacao
COMMENT 'Indicador Crianca Alfabetizada - camada Gold'
LOCATION 's3://SUA-CONTA-tc2-data-spec/gold/';

-- ----------------------------------------------------------------------------
-- Tabela principal: indicador por municipio
-- ----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS tc2_alfabetizacao.gold_indicador_municipio (
    id_municipio              int,
    nome_municipio            string,
    sigla_uf                  string,
    nome_uf                   string,
    nome_regiao               string,
    nome_mesorregiao          string,
    rede                      int,
    rede_nome                 string,
    serie                     int,
    taxa_alfabetizacao        double,
    media_portugues           double,
    percentual_participacao   double,
    meta_taxa_ano             double,
    meta_taxa_2030            double,
    gap_meta_ano              double,
    gap_meta_2030             double,
    taxa_uf                   double,
    taxa_brasil_publica       double,
    dif_vs_uf                 double,
    dif_vs_brasil             double,
    risco_alfabetizacao       string,
    situacao_meta             string,
    ritmo_anual_necessario_pp double,
    posicao_na_uf             int,
    quartil_nacional          int
)
PARTITIONED BY (ano int)
STORED AS PARQUET
LOCATION 's3://SUA-CONTA-tc2-data-spec/gold/gold_indicador_municipio/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

MSCK REPAIR TABLE tc2_alfabetizacao.gold_indicador_municipio;

-- ----------------------------------------------------------------------------
-- Consolidado meta x realizado por UF
-- ----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS tc2_alfabetizacao.gold_meta_vs_realizado (
    rede                    int,
    rede_nome               string,
    sigla_uf                string,
    nome_regiao             string,
    municipios              bigint,
    taxa_media              double,
    meta_media              double,
    gap_medio_pp            double,
    gap_medio_2030_pp       double,
    pct_municipios_na_meta  double,
    municipios_criticos     bigint,
    participacao_media      double
)
PARTITIONED BY (ano int)
STORED AS PARQUET
LOCATION 's3://SUA-CONTA-tc2-data-spec/gold/gold_meta_vs_realizado/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

MSCK REPAIR TABLE tc2_alfabetizacao.gold_meta_vs_realizado;

-- ============================================================================
-- CONSULTAS DE NEGÓCIO
-- ============================================================================

-- 1. Municípios em risco crítico no ciclo mais recente (prioridade de intervenção)
SELECT sigla_uf, nome_municipio, taxa_alfabetizacao, meta_taxa_ano, gap_meta_2030
FROM tc2_alfabetizacao.gold_indicador_municipio
WHERE ano = 2024
  AND rede_nome = 'Municipal'
  AND risco_alfabetizacao = 'Critico'
ORDER BY taxa_alfabetizacao ASC
LIMIT 100;

-- 2. Ranking de estados por percentual de municípios dentro da meta
SELECT sigla_uf, nome_regiao, municipios, taxa_media, pct_municipios_na_meta, municipios_criticos
FROM tc2_alfabetizacao.gold_meta_vs_realizado
WHERE ano = 2024 AND rede_nome = 'Municipal'
ORDER BY pct_municipios_na_meta DESC;

-- 3. Esforço anual necessário até 2030, por região
SELECT nome_regiao,
       COUNT(*)                             AS municipios,
       ROUND(AVG(taxa_alfabetizacao), 2)    AS taxa_media,
       ROUND(AVG(ritmo_anual_necessario_pp), 2) AS ritmo_anual_necessario_pp
FROM tc2_alfabetizacao.gold_indicador_municipio
WHERE ano = 2024 AND rede_nome = 'Municipal'
GROUP BY nome_regiao
ORDER BY ritmo_anual_necessario_pp DESC;

-- 4. Municípios que estão acima da meta do ano mas abaixo da média da própria UF
--    (bom no absoluto, ruim no relativo — recorte útil para focalizar apoio técnico)
SELECT sigla_uf, nome_municipio, taxa_alfabetizacao, taxa_uf, dif_vs_uf
FROM tc2_alfabetizacao.gold_indicador_municipio
WHERE ano = 2024 AND rede_nome = 'Municipal'
  AND situacao_meta = 'Meta atingida'
  AND dif_vs_uf < 0
ORDER BY dif_vs_uf ASC
LIMIT 50;

-- ============================================================================
-- VIEW pronta para dashboard (QuickSight / Power BI)
-- ============================================================================
CREATE OR REPLACE VIEW tc2_alfabetizacao.vw_painel_alfabetizacao AS
SELECT ano, sigla_uf, nome_regiao, nome_municipio, rede_nome,
       taxa_alfabetizacao, meta_taxa_ano, gap_meta_ano, gap_meta_2030,
       risco_alfabetizacao, situacao_meta, posicao_na_uf
FROM tc2_alfabetizacao.gold_indicador_municipio
WHERE rede_nome IN ('Municipal', 'Publica', 'Pública');