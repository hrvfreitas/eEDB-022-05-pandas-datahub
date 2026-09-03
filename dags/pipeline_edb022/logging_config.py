"""
Configuração central de logging do pipeline EDB022.

Por que um módulo próprio em vez de `print()`:
  - O Airflow já captura a saída do logger raiz do Python nos logs de cada
    task (Grid/Log view da UI), então usar `logging` (em vez de `print`)
    dá, de graça, timestamp, nível (INFO/WARNING/ERROR) e o nome do módulo
    de origem em cada linha — essencial para depurar quando algo falha.
  - Permite subir o nível de severidade (ex.: avisos de dados ausentes)
    sem misturar tudo como texto solto no stdout.
  - Um único ponto para trocar o formato/nível de log do projeto inteiro.

Uso em cada módulo do pipeline:

    from .logging_config import get_logger
    logger = get_logger(__name__)
    logger.info("...")
"""
import logging
import os

# Nível de log configurável via variável de ambiente (padrão: INFO).
# Ex.: LOG_LEVEL=DEBUG para depuração mais detalhada.
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

_FORMATO = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DATA_FORMATO = "%Y-%m-%d %H:%M:%S"

_configurado = False


def _configurar_uma_vez() -> None:
    """Configura o handler/formatador uma única vez por processo.

    Evita duplicar handlers (e, portanto, linhas de log repetidas) quando
    `get_logger` é chamado por vários módulos dentro da mesma task do
    Airflow. Não usamos `logging.basicConfig` direto porque o próprio
    Airflow já configura o logger raiz; em vez disso, configuramos o
    logger "pipeline_edb022" (pai de todos os loggers dos módulos) e
    deixamos a propagação normal do Python cuidar do resto.
    """
    global _configurado
    if _configurado:
        return

    logger_pacote = logging.getLogger("pipeline_edb022")
    logger_pacote.setLevel(LOG_LEVEL)

    # Se já existe um handler (ex.: o do próprio Airflow foi anexado a este
    # logger), não adiciona outro — só ajusta o nível.
    if not logger_pacote.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(_FORMATO, datefmt=_DATA_FORMATO))
        logger_pacote.addHandler(handler)
        # Evita que a mensagem seja emitida duas vezes (uma pelo handler
        # daqui, outra pelo handler que o Airflow anexa ao logger raiz).
        logger_pacote.propagate = False

    _configurado = True


def get_logger(nome_modulo: str) -> logging.Logger:
    """Retorna um logger nomeado (ex.: "pipeline_edb022.transform").

    Parâmetros:
      - nome_modulo: normalmente `__name__` do módulo chamador.
    """
    _configurar_uma_vez()
    return logging.getLogger(nome_modulo)
