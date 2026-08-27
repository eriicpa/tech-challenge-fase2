"""Funcoes para ler e gravar os parquets do data lake.

A coluna de particao fica no nome da pasta, e na leitura o pandas devolve ela
como Categorical, o que quebra operacao simples como .max(). Os tipos nullable
(Int16, Int32) tambem nao sobrevivem a ida e volta.
"""
from pathlib import Path

import pandas as pd


def preparar_particoes(df, particoes):
    """Converte as colunas de particao para tipos que aguentam a ida e volta."""
    dados = df.copy()
    for coluna in particoes or []:
        if pd.api.types.is_integer_dtype(dados[coluna]):
            dados[coluna] = dados[coluna].astype("int64")
        else:
            dados[coluna] = dados[coluna].astype(str)
    return dados


def ler_parquet(caminho):
    """Le um parquet (arquivo ou pasta particionada) e desfaz o Categorical."""
    df = pd.read_parquet(Path(caminho), engine="pyarrow")
    for coluna in df.columns:
        if isinstance(df[coluna].dtype, pd.CategoricalDtype):
            df[coluna] = df[coluna].astype(df[coluna].cat.categories.dtype)
    return df