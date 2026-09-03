"""
Constantes e utilitários compartilhados entre os módulos do pipeline.

Nesta versão "local", o pipeline funciona 100% em disco (volume Docker) e,
opcionalmente, também carrega cada camada num PostgreSQL local (schemas
raw/trusted/delivery), reproduzindo a arquitetura do repositório
`eEDB-022-02-fuzzy-main`. Nada disso depende da AWS:

  - UPLOAD_TO_S3 (padrão: false)      -> upload de cada camada para o S3
  - LOAD_TO_POSTGRES (padrão: true)   -> carga de cada camada no Postgres local

Ambos podem ser ligados/desligados via variável de ambiente, sem alterar
código.
"""
import os
import boto3
import pandas as pd

from .logging_config import get_logger

logger = get_logger(__name__)

# Paths locais (dentro do container do Airflow).
# Podem ser sobrescritos via variável de ambiente EDB022_BASE_DIR, mas o
# valor padrão já aponta para o volume montado pelo docker-compose local.
BASE_DIR = os.environ.get('EDB022_BASE_DIR', '/opt/airflow/Dados')
RAW_DIR = os.path.join(BASE_DIR, 'DataLake', 'Raw')
TRUSTED_DIR = os.path.join(BASE_DIR, 'DataLake', 'Trusted')
REFINED_DIR = os.path.join(BASE_DIR, 'DataLake', 'Refined')
CREDENTIALS_FILE = os.environ.get(
    'AWS_SHARED_CREDENTIALS_FILE', '/opt/airflow/credentials/credentials'
)

# --- S3 (opcional, desligado por padrão nesta versão local) ---
S3_BUCKET = os.environ.get('S3_BUCKET', 'edb022-datalake-089445119491')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
UPLOAD_TO_S3 = os.environ.get('UPLOAD_TO_S3', 'false').lower() == 'true'

# --- Postgres de dados (opcional, ligado por padrão nesta versão local) ---
LOAD_TO_POSTGRES = os.environ.get('LOAD_TO_POSTGRES', 'true').lower() == 'true'

# Score mínimo (0-100) para aceitar uma correspondência fuzzy automaticamente
# ao resolver CNPJ por nome (ver fuzzy_match.py). Mesmo valor do repositório
# eEDB-022-02-fuzzy-main.
FUZZY_SCORE_MINIMO = int(os.environ.get('FUZZY_SCORE_MINIMO', '83'))

# Garante que as camadas do Data Lake local existam antes de qualquer tarefa rodar.
for _dir in (RAW_DIR, TRUSTED_DIR, REFINED_DIR):
    os.makedirs(_dir, exist_ok=True)


def get_s3_client():
    """Retorna um client boto3 do S3, usando o arquivo de credenciais compartilhado."""
    if os.path.exists(CREDENTIALS_FILE):
        os.environ['AWS_SHARED_CREDENTIALS_FILE'] = CREDENTIALS_FILE
    return boto3.client('s3', region_name=AWS_REGION)


def upload_to_s3(local_file: str, s3_key: str) -> None:
    """Sobe um arquivo local para o bucket S3 do Data Lake (opcional).

    Em modo local (UPLOAD_TO_S3=false, padrão), a função apenas registra
    que o arquivo já está disponível no Data Lake local e não faz nada além
    disso — o pipeline continua funcionando sem AWS.
    """
    if not UPLOAD_TO_S3:
        logger.info("[modo local] Upload ao S3 desabilitado — arquivo mantido em: %s", local_file)
        return

    try:
        s3 = get_s3_client()
        logger.info("Enviando %s para s3://%s/%s...", local_file, S3_BUCKET, s3_key)
        s3.upload_file(local_file, S3_BUCKET, s3_key)
        logger.info("Upload concluído: s3://%s/%s", S3_BUCKET, s3_key)
    except Exception:
        logger.exception("Falha ao subir %s para s3://%s/%s", local_file, S3_BUCKET, s3_key)
        raise


def load_df_to_postgres(df: pd.DataFrame, table: str, schema: str) -> None:
    """Carrega um DataFrame numa tabela do Postgres de dados (opcional).

    Cria o schema se necessário e substitui a tabela (if_exists='replace'),
    igual ao comportamento do repositório eEDB-022-02-fuzzy-main. Em modo
    local sem Postgres de dados (LOAD_TO_POSTGRES=false), só avisa e segue —
    o Parquet gravado em disco continua sendo a fonte de verdade.
    """
    if not LOAD_TO_POSTGRES:
        logger.info(
            "[modo local] Carga no Postgres desabilitada — mantendo apenas Parquet para %s.%s",
            schema, table,
        )
        return

    from .db import get_engine  # import tardio: só precisa de sqlalchemy/psycopg2 se for usado

    try:
        engine = get_engine()
        with engine.begin() as conn:
            conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        df.to_sql(table, engine, schema=schema, if_exists='replace', index=False)
        logger.info("Tabela %s.%s carregada no Postgres (%d linhas).", schema, table, len(df))
    except Exception:
        logger.exception("Falha ao carregar %s.%s no Postgres", schema, table)
        raise
