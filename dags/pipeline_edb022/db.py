"""
Conexão com o PostgreSQL "de dados" (schemas raw / trusted / delivery).

Este banco é separado do Postgres usado pelo próprio Airflow para guardar
seus metadados (DAGs, execuções, etc.) — aqui só guardamos as tabelas do
pipeline EDB022, exatamente como no repositório original
`eEDB-022-02-fuzzy-main` (schemas raw/trusted/delivery).

Os valores podem ser sobrescritos por variáveis de ambiente — no
docker-compose local, DW_DB_HOST aponta para o serviço "postgres-dw" da
rede Docker. Quando LOAD_TO_POSTGRES=false (ver common.py), este módulo
nem chega a ser importado pelas etapas do pipeline.
"""
import os
import time

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from .logging_config import get_logger

logger = get_logger(__name__)

DB_USER = os.environ.get("DW_DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DW_DB_PASSWORD", "postgres")
DB_HOST = os.environ.get("DW_DB_HOST", "postgres-dw")
DB_PORT = os.environ.get("DW_DB_PORT", "5432")
DB_NAME = os.environ.get("DW_DB_NAME", "case_dados")

CONN_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_engine():
    """Retorna uma engine SQLAlchemy conectada ao Postgres de dados do projeto."""
    return create_engine(CONN_STRING)


def wait_for_db(max_tentativas: int = 30, intervalo_segundos: float = 2.0):
    """Aguarda o Postgres de dados ficar pronto para aceitar conexões."""
    engine = get_engine()
    for tentativa in range(1, max_tentativas + 1):
        try:
            with engine.connect() as conn:
                conn.exec_driver_sql("SELECT 1")
            logger.info("Conectado ao Postgres (dados) em %s:%s/%s.", DB_HOST, DB_PORT, DB_NAME)
            return
        except OperationalError:
            logger.warning(
                "Postgres (dados) ainda não disponível em %s:%s (tentativa %d/%d)...",
                DB_HOST, DB_PORT, tentativa, max_tentativas,
            )
            time.sleep(intervalo_segundos)
    logger.error(
        "Não foi possível conectar ao Postgres (dados) em %s:%s após %d tentativas.",
        DB_HOST, DB_PORT, max_tentativas,
    )
    raise RuntimeError(
        f"Não foi possível conectar ao Postgres (dados) em {DB_HOST}:{DB_PORT} "
        f"após {max_tentativas} tentativas."
    )
