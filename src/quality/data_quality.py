"""Verificacoes de qualidade dos dados.

Uso:
    dq = DataQualityCheck(df, "indicador_municipio", "bronze")
    dq.check_row_count(1000).check_not_null(["ano"]).relatorio()

Severidade ERROR conta como falha e faz o aprovar_ou_falhar() parar a pipeline.
WARNING so aparece no relatorio.
"""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

try:
    from tabulate import tabulate
except ImportError:
    tabulate = None

log = logging.getLogger("pipeline.quality")


class DataQualityCheck:

    def __init__(self, df, entidade, camada, output_dir="data/lake/quality"):
        self.df = df
        self.entidade = entidade
        self.camada = camada
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.resultados = []
        self.passou = 0
        self.falhou = 0
        self.avisos = 0

    def _registrar(self, verificacao, dimensao, ok, detalhe, severidade="ERROR"):
        if ok:
            status = "PASS"
            self.passou += 1
        elif severidade == "WARNING":
            status = "WARN"
            self.avisos += 1
        else:
            status = "FAIL"
            self.falhou += 1
        self.resultados.append({"verificacao": verificacao, "dimensao": dimensao,
                                "status": status, "detalhe": detalhe,
                                "severidade": severidade})
        return self

    def _tem_coluna(self, coluna, verificacao, dimensao, severidade):
        if coluna in self.df.columns:
            return True
        self._registrar(verificacao, dimensao, False, "coluna inexistente", severidade)
        return False

    # --- completude ---
    def check_row_count(self, minimo, severidade="ERROR"):
        total = len(self.df)
        return self._registrar("row_count", "COMPLETUDE", total >= minimo,
                               f"{total} linhas (minimo esperado: {minimo})", severidade)

    def check_not_null(self, colunas, severidade="ERROR"):
        for coluna in colunas:
            if not self._tem_coluna(coluna, f"not_null:{coluna}", "COMPLETUDE", severidade):
                continue
            nulos = int(self.df[coluna].isna().sum())
            pct = nulos / len(self.df) * 100 if len(self.df) else 0
            self._registrar(f"not_null:{coluna}", "COMPLETUDE", nulos == 0,
                            f"{nulos} nulos ({pct:.2f}%)", severidade)
        return self

    # --- unicidade ---
    def check_unique(self, colunas, severidade="ERROR"):
        for coluna in colunas:
            if not self._tem_coluna(coluna, f"unique:{coluna}", "UNICIDADE", severidade):
                continue
            duplicadas = int(self.df[coluna].duplicated().sum())
            self._registrar(f"unique:{coluna}", "UNICIDADE", duplicadas == 0,
                            f"{duplicadas} duplicatas", severidade)
        return self

    def check_chave_composta(self, colunas, severidade="ERROR"):
        faltantes = [c for c in colunas if c not in self.df.columns]
        if faltantes:
            return self._registrar("chave_composta", "UNICIDADE", False,
                                   f"colunas ausentes: {faltantes}", severidade)
        duplicadas = int(self.df.duplicated(subset=colunas).sum())
        return self._registrar(f"chave_composta:{'+'.join(colunas)}", "UNICIDADE",
                               duplicadas == 0, f"{duplicadas} linhas duplicadas no grao",
                               severidade)

    # --- validade ---
    def check_domain(self, coluna, valores_validos, severidade="ERROR"):
        if not self._tem_coluna(coluna, f"dominio:{coluna}", "VALIDADE", severidade):
            return self
        fora = self.df.loc[~self.df[coluna].isin(valores_validos) & self.df[coluna].notna(), coluna]
        exemplos = sorted(pd.unique(fora))[:5]
        detalhe = f"{len(fora)} valores fora do dominio {list(valores_validos)}"
        if exemplos:
            detalhe += f" | ex.: {exemplos}"
        return self._registrar(f"dominio:{coluna}", "VALIDADE", len(fora) == 0, detalhe, severidade)

    def check_range(self, coluna, minimo=None, maximo=None, severidade="ERROR"):
        if not self._tem_coluna(coluna, f"faixa:{coluna}", "VALIDADE", severidade):
            return self
        serie = pd.to_numeric(self.df[coluna], errors="coerce").dropna()
        fora = 0
        if minimo is not None:
            fora += int((serie < minimo).sum())
        if maximo is not None:
            fora += int((serie > maximo).sum())
        return self._registrar(f"faixa:{coluna}", "VALIDADE", fora == 0,
                               f"{fora} valores fora de [{minimo}, {maximo}]", severidade)

    # --- consistencia ---
    def check_referential_integrity(self, coluna, df_referencia, coluna_referencia,
                                    severidade="ERROR"):
        if not self._tem_coluna(coluna, f"fk:{coluna}", "CONSISTENCIA", severidade):
            return self
        validos = set(df_referencia[coluna_referencia].dropna().unique())
        presentes = self.df[coluna].dropna()
        orfaos = presentes[~presentes.isin(validos)]
        detalhe = f"{len(orfaos)} registros orfaos"
        exemplos = sorted(pd.unique(orfaos))[:5]
        if exemplos:
            detalhe += f" | ex.: {exemplos}"
        return self._registrar(f"fk:{coluna}->{coluna_referencia}", "CONSISTENCIA",
                               len(orfaos) == 0, detalhe, severidade)

    def check_soma_igual(self, colunas, valor_esperado, tolerancia=0.5, severidade="WARNING"):
        faltantes = [c for c in colunas if c not in self.df.columns]
        if faltantes:
            return self._registrar("soma_colunas", "CONSISTENCIA", False,
                                   f"colunas ausentes: {faltantes}", severidade)
        subset = self.df[colunas].dropna(how="all")
        if subset.empty:
            return self._registrar(f"soma:{len(colunas)}col", "CONSISTENCIA", True,
                                   "nenhuma linha preenchida", "WARNING")
        desvio = (subset.sum(axis=1) - valor_esperado).abs()
        fora = int((desvio > tolerancia).sum())
        return self._registrar(f"soma:{len(colunas)}col={valor_esperado}", "CONSISTENCIA",
                               fora == 0,
                               f"{fora}/{len(subset)} linhas fora da tolerancia +-{tolerancia}",
                               severidade)

    def check_cross_table(self, df_referencia, chaves, coluna, coluna_referencia,
                          tolerancia=0.01, severidade="ERROR"):
        # compara o mesmo indicador vindo de duas tabelas diferentes
        faltantes = [c for c in chaves if c not in self.df.columns
                     or c not in df_referencia.columns]
        if faltantes:
            return self._registrar("cross_table", "CONSISTENCIA", False,
                                   f"chaves ausentes: {faltantes}", severidade)
        juncao = (self.df[chaves + [coluna]]
                  .merge(df_referencia[chaves + [coluna_referencia]], on=chaves, how="inner")
                  .dropna(subset=[coluna, coluna_referencia]))
        if juncao.empty:
            return self._registrar(f"cross_table:{coluna}", "CONSISTENCIA", False,
                                   "nenhuma linha em comum", severidade)
        divergentes = int(((juncao[coluna] - juncao[coluna_referencia]).abs() > tolerancia).sum())
        pct = divergentes / len(juncao) * 100
        return self._registrar(
            f"cross_table:{coluna}~{coluna_referencia}", "CONSISTENCIA", divergentes == 0,
            f"{divergentes}/{len(juncao)} divergencias ({pct:.2f}%) acima de +-{tolerancia}",
            severidade)

    def check_freshness(self, coluna="_ingestion_date", dias_maximos=1, severidade="WARNING"):
        if not self._tem_coluna(coluna, f"freshness:{coluna}", "ATUALIDADE", severidade):
            return self
        datas = pd.to_datetime(self.df[coluna], errors="coerce").dropna()
        if datas.empty:
            return self._registrar(f"freshness:{coluna}", "ATUALIDADE", False,
                                   "sem datas validas", severidade)
        hoje = pd.Timestamp(datetime.now(timezone.utc).date())
        atraso = (hoje - datas.max().normalize()).days
        return self._registrar(f"freshness:{coluna}", "ATUALIDADE", atraso <= dias_maximos,
                               f"ingestao mais recente ha {atraso} dia(s)", severidade)

    def check_expectativa(self, nome, condicao_ok, detalhe, dimensao="REGRA_NEGOCIO",
                          severidade="ERROR"):
        # para regras especificas que nao cabem nos checks acima
        return self._registrar(nome, dimensao, bool(condicao_ok), detalhe, severidade)

    # --- saida ---
    @property
    def score(self):
        total = self.passou + self.falhou + self.avisos
        return round(self.passou / total * 100, 1) if total else 0.0

    def relatorio(self, salvar=True, imprimir=True):
        linhas = [(r["status"], r["dimensao"], r["verificacao"], r["detalhe"])
                  for r in self.resultados]
        if imprimir:
            print("\n" + "=" * 96)
            print(f"  QUALIDADE - {self.entidade.upper()} [{self.camada.upper()}]")
            print("=" * 96)
            if tabulate:
                print(tabulate(linhas, headers=["Status", "Dimensao", "Verificacao", "Detalhe"],
                               tablefmt="rounded_outline"))
            else:
                print(pd.DataFrame(linhas, columns=["Status", "Dimensao", "Verificacao",
                                                    "Detalhe"]).to_string(index=False))
            print(f"  Score: {self.score}%  |  PASS {self.passou}  FAIL {self.falhou}  "
                  f"WARN {self.avisos}")

        dados = {"entidade": self.entidade, "camada": self.camada, "score": self.score,
                 "linhas_avaliadas": len(self.df),
                 "gerado_em": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                 "resumo": {"pass": self.passou, "fail": self.falhou, "warn": self.avisos},
                 "verificacoes": self.resultados}
        if salvar:
            destino = self.output_dir / f"dq_{self.camada}_{self.entidade}.json"
            destino.write_text(json.dumps(dados, indent=2, ensure_ascii=False), encoding="utf-8")
            dados["arquivo"] = str(destino)

        log.info("[DQ:%s] %s | score=%s%% | PASS=%d FAIL=%d WARN=%d", self.camada.upper(),
                 self.entidade, self.score, self.passou, self.falhou, self.avisos)
        return dados

    def aprovar_ou_falhar(self):
        # se tiver falha critica, para a pipeline aqui
        if self.falhou:
            criticas = [r for r in self.resultados if r["status"] == "FAIL"]
            detalhes = "; ".join(f"{r['verificacao']} ({r['detalhe']})" for r in criticas)
            raise ValueError(f"[QUALITY GATE] {self.entidade} [{self.camada}]: "
                             f"{self.falhou} verificacao(oes) critica(s) falharam -> {detalhes}")
        return self


def consolidar_relatorios(relatorios):
    """Junta varios relatorios num painel unico."""
    return pd.DataFrame([{
        "camada": r["camada"],
        "entidade": r["entidade"],
        "linhas": r["linhas_avaliadas"],
        "score_%": r["score"],
        "pass": r["resumo"]["pass"],
        "fail": r["resumo"]["fail"],
        "warn": r["resumo"]["warn"],
    } for r in relatorios]).sort_values(["camada", "entidade"]).reset_index(drop=True)