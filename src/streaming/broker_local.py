"""Um "Kafka de mentira" que roda em memoria.

Fiz isso pra conseguir rodar o notebook em qualquer maquina, porque instalar
Kafka de verdade so funciona no Colab/Linux. Tem so o que a pipeline usa:
topicos, particoes por hash da chave, offsets, consumer group e lag.

Os metodos tem os mesmos nomes do kafka-python (send, flush, commit...), entao
o resto do notebook nao precisa saber qual dos dois esta rodando.
"""
import json
import zlib
from collections import defaultdict
from datetime import datetime, timezone


class Mensagem:
    def __init__(self, topic, partition, offset, key, value, timestamp):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.key = key
        self.value = value
        self.timestamp = timestamp


class MetadadosEnvio:
    def __init__(self, topic, partition, offset):
        self.topic = topic
        self.partition = partition
        self.offset = offset


class FuturoEnvio:
    """Imita o Future que o KafkaProducer.send() devolve."""

    def __init__(self, metadados):
        self._metadados = metadados

    def get(self, timeout=None):
        return self._metadados


class ProdutorLocal:

    def __init__(self, broker):
        self.broker = broker
        self.enviados = 0

    def send(self, topic, value=None, key=None):
        # serializa igual o Kafka faria, pra nao mascarar erro de serializacao
        valor = json.dumps(value).encode("utf-8")
        chave = str(key).encode("utf-8") if key is not None else None
        particao, offset = self.broker.produzir(topic, chave, valor)
        self.enviados += 1
        return FuturoEnvio(MetadadosEnvio(topic, particao, offset))

    def flush(self, timeout=None):
        return None


class ConsumidorLocal:

    def __init__(self, broker, topicos, group_id, auto_offset_reset="earliest",
                 consumer_timeout_ms=5000):
        self.broker = broker
        self.topicos = list(topicos)
        self.group_id = group_id

        # onde comecar a ler: do offset commitado, ou do inicio/fim se o grupo e novo
        self.posicoes = {}
        for topico in self.topicos:
            for particao in range(self.broker.numero_particoes(topico)):
                commitado = self.broker.offset_commitado(group_id, topico, particao)
                if commitado is not None:
                    inicio = commitado
                elif auto_offset_reset == "earliest":
                    inicio = 0
                else:
                    inicio = self.broker.offset_final(topico, particao)
                self.posicoes[(topico, particao)] = inicio

    def __iter__(self):
        for (topico, particao), posicao in sorted(self.posicoes.items()):
            for bruto in self.broker.ler(topico, particao, posicao):
                self.posicoes[(topico, particao)] = bruto["offset"] + 1
                yield Mensagem(
                    topic=topico,
                    partition=particao,
                    offset=bruto["offset"],
                    key=bruto["key"].decode("utf-8") if bruto["key"] else None,
                    value=json.loads(bruto["value"].decode("utf-8")),
                    timestamp=bruto["timestamp"],
                )

    def commit(self):
        for (topico, particao), posicao in self.posicoes.items():
            self.broker.commitar(self.group_id, topico, particao, posicao)

    def close(self):
        return None


class BrokerLocal:

    def __init__(self):
        self.topicos = {}
        self.commits = defaultdict(dict)

    def criar_topico(self, nome, particoes=1, replicacao=1):
        if nome not in self.topicos:
            self.topicos[nome] = {"particoes": particoes, "replicacao": replicacao,
                                  "dados": [[] for _ in range(particoes)]}
        return self.topicos[nome]

    def listar_topicos(self):
        return sorted(self.topicos)

    def numero_particoes(self, topico):
        return self.topicos[topico]["particoes"]

    def descrever(self, topico):
        info = self.topicos[topico]
        return [{"topico": topico, "particao": i, "mensagens": len(fila),
                 "ultimo_offset": len(fila) - 1 if fila else -1,
                 "replicacao": info["replicacao"]}
                for i, fila in enumerate(info["dados"])]

    def produzir(self, topico, chave, valor):
        if topico not in self.topicos:
            self.criar_topico(topico)
        # mesma chave sempre cai na mesma particao (e o que garante a ordem)
        total = self.numero_particoes(topico)
        particao = zlib.crc32(chave) % total if chave else 0

        fila = self.topicos[topico]["dados"][particao]
        offset = len(fila)
        fila.append({"offset": offset, "key": chave, "value": valor,
                     "timestamp": datetime.now(timezone.utc).timestamp()})
        return particao, offset

    def ler(self, topico, particao, desde_offset):
        return list(self.topicos[topico]["dados"][particao][desde_offset:])

    def offset_final(self, topico, particao):
        return len(self.topicos[topico]["dados"][particao])

    def commitar(self, grupo, topico, particao, offset):
        self.commits[grupo][(topico, particao)] = offset

    def offset_commitado(self, grupo, topico, particao):
        return self.commits.get(grupo, {}).get((topico, particao))

    def lag(self, grupo, topico):
        linhas = []
        for particao in range(self.numero_particoes(topico)):
            final = self.offset_final(topico, particao)
            atual = self.offset_commitado(grupo, topico, particao) or 0
            linhas.append({"topico": topico, "particao": particao, "grupo": grupo,
                           "offset_final": final, "offset_consumidor": atual,
                           "lag": final - atual})
        return linhas

    def produtor(self, **kwargs):
        return ProdutorLocal(self)

    def consumidor(self, topicos, group_id, **kwargs):
        return ConsumidorLocal(self, topicos, group_id, **kwargs)