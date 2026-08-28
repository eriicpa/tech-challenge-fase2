"""AWS Lambda — processador de eventos de avaliação (gatilho Amazon MSK).

É a versão gerenciada do stream processor do Notebook 02: valida contra o
contrato, roteia inválidos para a DLQ (SQS), enriquece e publica alertas no SNS.
Os eventos válidos seguem para o Firehose, que faz o buffer e escreve Parquet
particionado no S3 — sem código de micro-batch e sem small files.

Variáveis de ambiente:
    DLQ_URL              URL da fila SQS de dead letter
    SNS_TOPIC_ARN        tópico de alertas
    FIREHOSE_STREAM      nome do delivery stream para a Bronze
    TABELA_MUNICIPIOS    tabela DynamoDB com a dimensão territorial (cache)
"""
import base64
import json
import os
from datetime import datetime, timezone

import boto3

PONTO_CORTE_SAEB = 743
CAMPOS_OBRIGATORIOS = ["id_evento", "tipo_evento", "ts_evento", "id_municipio",
                       "rede", "proficiencia_portugues"]

sqs = boto3.client("sqs")
sns = boto3.client("sns")
firehose = boto3.client("firehose")
dynamodb = boto3.resource("dynamodb")

DLQ_URL          = os.environ["DLQ_URL"]
SNS_TOPIC_ARN    = os.environ["SNS_TOPIC_ARN"]
FIREHOSE_STREAM  = os.environ["FIREHOSE_STREAM"]
tabela_municipios = dynamodb.Table(os.environ["TABELA_MUNICIPIOS"])

# Cache em memória do container: a dimensão territorial muda raramente, e uma
# leitura no DynamoDB por evento seria o maior custo da função.
_cache_municipios = {}


def buscar_municipio(id_municipio):
    if id_municipio not in _cache_municipios:
        resposta = tabela_municipios.get_item(Key={"id_municipio": int(id_municipio)})
        _cache_municipios[id_municipio] = resposta.get("Item")
    return _cache_municipios[id_municipio]


def validar(evento):
    faltantes = [c for c in CAMPOS_OBRIGATORIOS if evento.get(c) is None]
    if faltantes:
        return False, "campos_obrigatorios_ausentes:" + ",".join(faltantes)
    try:
        proficiencia = float(evento["proficiencia_portugues"])
    except (TypeError, ValueError):
        return False, "proficiencia_nao_numerica"
    if not 200 <= proficiencia <= 1000:
        return False, "proficiencia_fora_de_faixa:%s" % proficiencia
    if buscar_municipio(evento["id_municipio"]) is None:
        return False, "municipio_inexistente:%s" % evento["id_municipio"]
    return True, None


def lambda_handler(event, context):
    """Gatilho MSK: `event['records']` traz lotes por partição do tópico."""
    validos, rejeitados = [], 0

    for _particao, mensagens in event.get("records", {}).items():
        for mensagem in mensagens:
            bruto = json.loads(base64.b64decode(mensagem["value"]).decode("utf-8"))
            ok, motivo = validar(bruto)

            if not ok:
                rejeitados += 1
                sqs.send_message(QueueUrl=DLQ_URL, MessageBody=json.dumps({
                    "evento_original": bruto,
                    "motivo_rejeicao": motivo,
                    "ts_rejeicao": datetime.now(timezone.utc).isoformat(),
                }))
                continue

            territorio = buscar_municipio(bruto["id_municipio"])
            enriquecido = dict(bruto)
            enriquecido.update({
                "nome_municipio": territorio["nome_municipio"],
                "sigla_uf": territorio["sigla_uf"],
                "nome_regiao": territorio["nome_regiao"],
                "alfabetizado": float(bruto["proficiencia_portugues"]) >= PONTO_CORTE_SAEB,
                "ts_processamento": datetime.now(timezone.utc).isoformat(),
            })
            validos.append({"Data": (json.dumps(enriquecido) + "\n").encode("utf-8")})

    # Firehose faz o buffer por tempo/tamanho e converte para Parquet no S3.
    for inicio in range(0, len(validos), 500):        # limite da API: 500 registros por chamada
        firehose.put_record_batch(DeliveryStreamName=FIREHOSE_STREAM,
                                  Records=validos[inicio:inicio + 500])

    total = len(validos) + rejeitados
    taxa_dlq = (rejeitados / total * 100) if total else 0
    if taxa_dlq > 10:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="[ALERTA] Taxa de rejeicao acima do limite",
            Message=json.dumps({"tipo": "TAXA_DLQ_ALTA", "taxa_dlq_pct": round(taxa_dlq, 2),
                                "eventos": total, "rejeitados": rejeitados}),
        )

    return {"processados": total, "validos": len(validos), "rejeitados": rejeitados}