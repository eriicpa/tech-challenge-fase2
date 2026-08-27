"""Guarda as metricas de cada etapa da pipeline: tempo, linhas e erros."""
import json
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

log = logging.getLogger("pipeline.monitor")


class PipelineMonitor:

    def __init__(self, pipeline, run_id, output_dir="data/lake/monitoring",
                 sla_etapa_segundos=120.0, limite_rejeicao_pct=5.0):
        self.pipeline = pipeline
        self.run_id = run_id
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.sla_etapa_segundos = sla_etapa_segundos
        self.limite_rejeicao_pct = limite_rejeicao_pct
        self.inicio = time.perf_counter()
        self.etapas = []
        self.alertas = []

    @contextmanager
    def etapa(self, nome, camada="-", sla_segundos=None):
        # usar com "with": dentro do bloco a gente preenche linhas_entrada/linhas_saida
        contexto = {"etapa": nome, "camada": camada, "linhas_entrada": 0,
                    "linhas_saida": 0, "linhas_rejeitadas": 0, "bytes_gerados": 0,
                    "detalhe": ""}
        t0 = time.perf_counter()
        status, erro = "OK", None
        try:
            yield contexto
        except Exception as exc:
            status = "FALHA"
            erro = f"{type(exc).__name__}: {exc}"
            self.registrar_alerta("CRITICAL", f"Falha na etapa '{nome}': {erro}")
            raise
        finally:
            duracao = round(time.perf_counter() - t0, 3)
            registro = dict(contexto)
            registro["status"] = status
            registro["erro"] = erro
            registro["duracao_segundos"] = duracao
            registro["timestamp"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

            entrada = registro["linhas_entrada"] or registro["linhas_saida"]
            registro["throughput_linhas_s"] = round(entrada / duracao, 1) if duracao else 0.0
            registro["taxa_rejeicao_pct"] = (
                round(registro["linhas_rejeitadas"] / entrada * 100, 2) if entrada else 0.0)
            self.etapas.append(registro)

            if status == "OK":
                log.info("[MONITOR] %-38s | %-6s | %7.3fs | in=%s out=%s rej=%s",
                         nome, camada, duracao, registro["linhas_entrada"],
                         registro["linhas_saida"], registro["linhas_rejeitadas"])

            limite = sla_segundos or self.sla_etapa_segundos
            if duracao > limite:
                self.registrar_alerta("WARNING", f"Etapa '{nome}' levou {duracao}s (SLA {limite}s)")
            if registro["taxa_rejeicao_pct"] > self.limite_rejeicao_pct:
                self.registrar_alerta(
                    "WARNING",
                    f"Etapa '{nome}' rejeitou {registro['taxa_rejeicao_pct']}% das linhas")

    def registrar_alerta(self, severidade, mensagem):
        self.alertas.append({"severidade": severidade, "mensagem": mensagem,
                             "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds")})
        if severidade == "CRITICAL":
            log.error("[ALERTA:%s] %s", severidade, mensagem)
        elif severidade == "WARNING":
            log.warning("[ALERTA:%s] %s", severidade, mensagem)
        else:
            log.info("[ALERTA:%s] %s", severidade, mensagem)

    def resumo(self):
        colunas = ["etapa", "camada", "status", "duracao_segundos", "linhas_entrada",
                   "linhas_saida", "linhas_rejeitadas", "taxa_rejeicao_pct",
                   "throughput_linhas_s"]
        if not self.etapas:
            return pd.DataFrame(columns=colunas)
        return pd.DataFrame(self.etapas)[colunas]

    def metricas_gerais(self):
        df = self.resumo()
        return {
            "pipeline": self.pipeline,
            "run_id": self.run_id,
            "duracao_total_segundos": round(time.perf_counter() - self.inicio, 3),
            "etapas_executadas": len(df),
            "etapas_com_falha": int((df["status"] == "FALHA").sum()) if len(df) else 0,
            "linhas_processadas": int(df["linhas_saida"].sum()) if len(df) else 0,
            "linhas_rejeitadas": int(df["linhas_rejeitadas"].sum()) if len(df) else 0,
            "alertas": len(self.alertas),
            "alertas_criticos": sum(1 for a in self.alertas if a["severidade"] == "CRITICAL"),
        }

    def salvar(self):
        dados = self.metricas_gerais()
        dados["gerado_em"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        dados["etapas"] = self.etapas
        dados["alertas"] = self.alertas

        destino = self.output_dir / f"run_{self.pipeline}_{self.run_id}.json"
        destino.write_text(json.dumps(dados, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info("[MONITOR] Relatorio salvo em %s", destino)
        return destino

    def imprimir_painel(self):
        m = self.metricas_gerais()
        print("\n" + "=" * 78)
        print(f"  PAINEL DE EXECUCAO - {m['pipeline']} | run {m['run_id']}")
        print("=" * 78)
        print(self.resumo().to_string(index=False))
        print("-" * 78)
        print(f"  Duracao total ....... {m['duracao_total_segundos']}s")
        print(f"  Etapas .............. {m['etapas_executadas']} ({m['etapas_com_falha']} com falha)")
        print(f"  Linhas processadas .. {m['linhas_processadas']}")
        print(f"  Linhas rejeitadas ... {m['linhas_rejeitadas']}")
        print(f"  Alertas ............. {m['alertas']} ({m['alertas_criticos']} criticos)")
        if self.alertas:
            print("-" * 78)
            for a in self.alertas:
                print(f"  [{a['severidade']}] {a['mensagem']}")
        print("=" * 78)