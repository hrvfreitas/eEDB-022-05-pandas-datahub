"""
DAG: ingestao_dados_edb022

Orquestra o pipeline de ingestão, tratamento, qualidade e metadados em 5 etapas:
  1. extract_and_load_raw   — CSV/TSV locais -> Raw (Parquet local + Postgres schema "raw")
  2. transform_to_trusted   — Raw -> Trusted: limpeza, tipagem e resolução de
                               CNPJ por nome (com fallback de fuzzy matching,
                               RapidFuzz) -> Parquet local + Postgres schema "trusted"
  3. data_quality_checks    — Great Expectations sobre a camada Trusted
  4. build_refined_layer    — Trusted -> Delivery/Refined (join final por CNPJ)
                               -> Parquet local + Postgres schema "delivery"
  5. emit_metadata          — cataloga o schema de cada dataset (Raw/Trusted/
                               Delivery) no DataHub via API REST

O tratamento de dados (etapas 1 e 2) reproduz a lógica do repositório
`eEDB-022-02-fuzzy-main` (encoding/delimitador corretos por fonte,
deduplicação de bancos, resolução de CNPJ de conglomerados e de
empregadores por nome/fuzzy match). Upload para S3, carga no Postgres e
emissão de metadados para o DataHub são todos opcionais e configuráveis
via variável de ambiente (ver common.py e metadata.py).

A implementação de cada etapa vive em pipeline_edb022/*.py — este
arquivo cuida só da orquestração (Airflow).
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline_edb022.extract import extract_and_load_raw
from pipeline_edb022.transform import transform_to_trusted
from pipeline_edb022.quality import data_quality_checks
from pipeline_edb022.refined import build_refined_layer
from pipeline_edb022.metadata import emit_metadata
from pipeline_edb022.logging_config import get_logger

logger = get_logger(__name__)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='ingestao_dados_edb022',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['edb022', 'ingestion', 'fuzzy-match', 'postgres', 'datahub'],
) as dag:

    raw_task = PythonOperator(
        task_id='extract_and_load_raw',
        python_callable=extract_and_load_raw,
    )

    trusted_task = PythonOperator(
        task_id='transform_to_trusted',
        python_callable=transform_to_trusted,
    )

    quality_task = PythonOperator(
        task_id='data_quality_checks',
        python_callable=data_quality_checks,
    )

    refined_task = PythonOperator(
        task_id='build_refined_layer',
        python_callable=build_refined_layer,
    )

    metadata_task = PythonOperator(
        task_id='emit_metadata',
        python_callable=emit_metadata,
    )

    # Ordem: Raw -> Trusted -> Quality -> Refined/Delivery -> Metadados.
    # A validação roda sobre dados JÁ tipados/limpos da camada Trusted, não
    # sobre o Raw. Os metadados são emitidos por último, depois de todas as
    # camadas gravadas, para catalogar o resultado final de cada uma.
    raw_task >> trusted_task >> quality_task >> refined_task >> metadata_task
