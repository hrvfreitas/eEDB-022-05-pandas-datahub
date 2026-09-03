"""
Etapa 1 — Ingestão da camada Raw.

Lê cada fonte com o encoding/delimitador corretos (mesma lógica do
repositório `eEDB-022-02-fuzzy-main/src/01_ingest_raw.py`), sem nenhum
tratamento de conteúdo, e grava:

  - uma cópia em Parquet no Data Lake local (Raw/), todas as colunas como
    texto, tal como a fonte;
  - opcionalmente, a mesma tabela no Postgres de dados, schema "raw"
    (LOAD_TO_POSTGRES=true, padrão);
  - opcionalmente, upload para o S3 (UPLOAD_TO_S3=true).

Importante: ao contrário da extração "ingênua" anterior (que já lia os
CSVs com o pandas inferindo tipos), aqui TUDO é lido como string
(dtype=str, keep_default_na=False) — é isso que evita, por exemplo, que o
CNPJ do TSV de Bancos vire um número truncado (perdendo dígitos) só por
ter sido lido como int64. A tipagem correta acontece só na camada
Trusted (transform.py), coluna a coluna, de forma intencional.
"""
import glob
import os

import pandas as pd

from .common import BASE_DIR, RAW_DIR, upload_to_s3, load_df_to_postgres
from .logging_config import get_logger

logger = get_logger(__name__)


def _ler_reclamacoes_raw() -> pd.DataFrame:
    """Lê os arquivos trimestrais de reclamações (2021-2022), como texto puro."""
    arquivos = sorted(glob.glob(os.path.join(BASE_DIR, 'Reclamacoes', '*.csv')))
    dfs = []
    for arq in arquivos:
        if os.path.getsize(arq) == 0:
            # Ex.: 2022_tri_02_nao_ha_dados.csv -> trimestre sem divulgação pelo BACEN.
            logger.warning("Arquivo vazio, pulando: %s", os.path.basename(arq))
            continue
        df = pd.read_csv(arq, sep=';', encoding='latin1', dtype=str, keep_default_na=False)
        df['arquivo_origem'] = os.path.basename(arq)
        dfs.append(df)
        logger.debug("Lido %s (%d linhas)", os.path.basename(arq), len(df))

    if not dfs:
        logger.warning("Nenhum arquivo de Reclamações encontrado em %s", os.path.join(BASE_DIR, 'Reclamacoes'))
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def _ler_bancos_raw() -> pd.DataFrame:
    """Lê a tabela de enquadramento (segmento) dos bancos, como texto puro."""
    arq = os.path.join(BASE_DIR, 'Bancos', 'EnquadramentoInicia_v2.tsv')
    if not os.path.exists(arq):
        logger.error("Arquivo não encontrado, pulando: %s", arq)
        return pd.DataFrame()
    df = pd.read_csv(arq, sep='\t', encoding='latin1', dtype=str, keep_default_na=False)
    df['arquivo_origem'] = os.path.basename(arq)
    return df


def _ler_empregados_raw(nome_arquivo: str) -> pd.DataFrame:
    """Lê um dos arquivos de avaliação Glassdoor, como texto puro."""
    arq = os.path.join(BASE_DIR, 'Empregados', nome_arquivo)
    if not os.path.exists(arq):
        logger.error("Arquivo não encontrado, pulando: %s", arq)
        return pd.DataFrame()
    df = pd.read_csv(arq, sep='|', encoding='utf-8', dtype=str, keep_default_na=False)
    df['arquivo_origem'] = nome_arquivo
    return df


def _salvar_raw(df: pd.DataFrame, nome_arquivo_parquet: str, tabela_postgres: str) -> None:
    if df.empty:
        logger.warning("%s: sem dados, nada a salvar.", nome_arquivo_parquet)
        return
    local_path = os.path.join(RAW_DIR, nome_arquivo_parquet)
    df.to_parquet(local_path, index=False)
    logger.info("Parquet gravado: %s (%d linhas)", local_path, len(df))
    upload_to_s3(local_path, f'Raw/{nome_arquivo_parquet}')
    load_df_to_postgres(df, tabela_postgres, schema='raw')


def extract_and_load_raw():
    logger.info("Lendo Reclamações (arquivos trimestrais)...")
    reclamacoes = _ler_reclamacoes_raw()
    logger.info("  -> %d linhas", len(reclamacoes))
    _salvar_raw(reclamacoes, 'reclamacoes.parquet', 'reclamacoes')

    logger.info("Lendo Bancos (enquadramento/segmento)...")
    bancos = _ler_bancos_raw()
    logger.info("  -> %d linhas", len(bancos))
    _salvar_raw(bancos, 'bancos_enquadramento.parquet', 'bancos_enquadramento')

    logger.info("Lendo Empregados (Glassdoor - match)...")
    empregados_match = _ler_empregados_raw('glassdoor_consolidado_join_match_v2.csv')
    logger.info("  -> %d linhas", len(empregados_match))
    _salvar_raw(empregados_match, 'empregados_match.parquet', 'empregados_glassdoor_match')

    logger.info("Lendo Empregados (Glassdoor - match_less)...")
    empregados_match_less = _ler_empregados_raw('glassdoor_consolidado_join_match_less_v2.csv')
    logger.info("  -> %d linhas", len(empregados_match_less))
    _salvar_raw(empregados_match_less, 'empregados_match_less.parquet', 'empregados_glassdoor_match_less')
