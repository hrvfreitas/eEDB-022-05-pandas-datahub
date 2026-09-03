"""
Etapa 5 — Metadados (DataHub).

Registra cada dataset do pipeline (Raw, Trusted, Delivery) no catálogo do
DataHub via API REST — schema (colunas/tipos), linhagem por camada e uma
breve descrição de cada tabela ficam pesquisáveis no DataHub. Reproduz o
papel que o DataHub tinha no repositório original
(`edb022-ingestao-dados-aws-main`), onde ele catalogava o schema das
tabelas gravadas no S3; aqui ele cataloga o Data Lake local.

Por que o DataHub não está dentro do docker-compose.yaml deste projeto:
o stack oficial (`datahub docker quickstart`) sobe ~10 containers (Kafka,
Elasticsearch, MySQL, GMS, frontend, etc.) — pesado demais para incluir
sempre junto do Airflow. Em vez disso, o pipeline só EMITE metadados para
ele via API REST, exatamente como o projeto original fazia (DataHub
rodando à parte, na mesma máquina).

Controlado por variáveis de ambiente:
  - EMIT_METADATA_TO_DATAHUB (padrão: true)
  - DATAHUB_GMS_URL (padrão: http://localhost:8080 — ajuste para
    http://host.docker.internal:8080 se o DataHub estiver rodando fora
    da rede Docker do Airflow, no host)

Se o DataHub não estiver acessível, esta etapa AVISA e segue sem falhar
o pipeline: o catálogo de metadados é um "extra" de governança e não deve
travar a entrega dos dados (Trusted/Delivery já foram gravados antes
desta etapa rodar).
"""
import os

import pandas as pd

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DatasetPropertiesClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    NumberTypeClass,
)

from .common import RAW_DIR, TRUSTED_DIR, REFINED_DIR
from .logging_config import get_logger

logger = get_logger(__name__)

EMIT_METADATA_TO_DATAHUB = os.environ.get('EMIT_METADATA_TO_DATAHUB', 'true').lower() == 'true'
DATAHUB_GMS_URL = os.environ.get('DATAHUB_GMS_URL', 'http://localhost:8080')
DATAHUB_PLATFORM = 'file'  # representa os arquivos Parquet do Data Lake local
DATAHUB_ENV = 'PROD'

# (nome_no_datahub, caminho_do_parquet, camada, descrição)
DATASETS = [
    ('edb022.raw.reclamacoes', lambda: os.path.join(RAW_DIR, 'reclamacoes.parquet'),
     'raw', 'Reclamações do BACEN, camada Raw (texto puro, sem tratamento).'),
    ('edb022.raw.bancos_enquadramento', lambda: os.path.join(RAW_DIR, 'bancos_enquadramento.parquet'),
     'raw', 'Enquadramento/segmento dos bancos, camada Raw.'),
    ('edb022.raw.empregados_match', lambda: os.path.join(RAW_DIR, 'empregados_match.parquet'),
     'raw', 'Avaliações Glassdoor (arquivo match), camada Raw.'),
    ('edb022.raw.empregados_match_less', lambda: os.path.join(RAW_DIR, 'empregados_match_less.parquet'),
     'raw', 'Avaliações Glassdoor (arquivo match_less), camada Raw.'),
    ('edb022.trusted.bancos', lambda: os.path.join(TRUSTED_DIR, 'bancos.parquet'),
     'trusted', 'Bancos tratados e deduplicados por CNPJ (nome "- PRUDENCIAL" como oficial).'),
    ('edb022.trusted.reclamacoes', lambda: os.path.join(TRUSTED_DIR, 'reclamacoes.parquet'),
     'trusted', 'Reclamações tratadas, com CNPJ resolvido (direto, fuzzy match ou não encontrado).'),
    ('edb022.trusted.empregados_glassdoor', lambda: os.path.join(TRUSTED_DIR, 'empregados_glassdoor.parquet'),
     'trusted', 'Avaliações Glassdoor consolidadas (match + match_less), deduplicadas por CNPJ.'),
    ('edb022.delivery.tb_reclamacoes_bancos_funcionarios', lambda: os.path.join(REFINED_DIR, 'delivery_reclamacoes_bancos_funcionarios.parquet'),
     'delivery', 'Tabela final: reclamações x bancos x avaliações de funcionários, por CNPJ e trimestre.'),
]


def _tipo_datahub(dtype) -> SchemaFieldDataTypeClass:
    dtype_str = str(dtype)
    if 'bool' in dtype_str:
        return SchemaFieldDataTypeClass(type=BooleanTypeClass())
    if 'int' in dtype_str or 'float' in dtype_str or 'Int64' in dtype_str:
        return SchemaFieldDataTypeClass(type=NumberTypeClass())
    return SchemaFieldDataTypeClass(type=StringTypeClass())


def _emit_dataset(emitter: DatahubRestEmitter, nome_dataset: str, df: pd.DataFrame, descricao: str, camada: str) -> None:
    urn = make_dataset_urn(platform=DATAHUB_PLATFORM, name=nome_dataset, env=DATAHUB_ENV)

    campos = [
        SchemaFieldClass(
            fieldPath=col,
            type=_tipo_datahub(df[col].dtype),
            nativeDataType=str(df[col].dtype),
        )
        for col in df.columns
    ]
    schema_metadata = SchemaMetadataClass(
        schemaName=nome_dataset,
        platform=f'urn:li:dataPlatform:{DATAHUB_PLATFORM}',
        version=0,
        hash='',
        platformSchema=OtherSchemaClass(rawSchema=''),
        fields=campos,
    )

    propriedades = DatasetPropertiesClass(
        description=descricao,
        customProperties={
            'camada': camada,
            'linhas': str(len(df)),
            'colunas': str(len(df.columns)),
            'projeto': 'edb022-pipeline-local',
        },
    )

    for aspect in (schema_metadata, propriedades):
        emitter.emit(MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect))

    logger.info("[datahub] Metadados emitidos para %s (%d linhas, %d colunas)", urn, len(df), len(df.columns))


def emit_metadata() -> None:
    """Cataloga no DataHub o schema/descrição de cada dataset das camadas Raw, Trusted e Delivery."""
    if not EMIT_METADATA_TO_DATAHUB:
        logger.info(
            "[modo local] Emissão de metadados para o DataHub desabilitada (EMIT_METADATA_TO_DATAHUB=false)."
        )
        return

    try:
        emitter = DatahubRestEmitter(DATAHUB_GMS_URL)
        emitter.test_connection()
    except Exception:
        logger.warning(
            "[datahub] Não foi possível conectar ao DataHub em %s.\n"
            "          Suba-o com 'datahub docker quickstart' (ver README) para catalogar\n"
            "          os metadados. O pipeline de dados NÃO depende disso — Trusted e\n"
            "          Delivery já foram gravados normalmente nas etapas anteriores.",
            DATAHUB_GMS_URL,
            exc_info=True,
        )
        return

    algum_emitido = False
    for nome_dataset, caminho_fn, camada, descricao in DATASETS:
        caminho = caminho_fn()
        if not os.path.exists(caminho):
            logger.warning("[datahub] Arquivo não encontrado, pulando: %s", caminho)
            continue
        df = pd.read_parquet(caminho)
        try:
            _emit_dataset(emitter, nome_dataset, df, descricao, camada)
            algum_emitido = True
        except Exception:
            logger.exception("[datahub] Falha ao emitir metadados para %s", nome_dataset)

    emitter.flush()
    if algum_emitido:
        logger.info("Metadados emitidos para o DataHub com sucesso.")
    else:
        logger.warning("[datahub] Nenhum dataset encontrado para catalogar (rode as etapas anteriores primeiro).")
