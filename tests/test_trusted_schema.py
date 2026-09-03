"""
Testes básicos de schema da camada Trusted, já refletindo o tratamento de
dados unificado (ver dags/pipeline_edb022/transform.py).

Execução: pytest tests/test_trusted_schema.py
"""
import os
import pandas as pd
import pytest

TRUSTED_DIR = os.environ.get('EDB022_TRUSTED_DIR', '/opt/airflow/Dados/DataLake/Trusted')

RECLAMACOES_EXPECTED_COLUMNS = {
    'ano',
    'trimestre',
    'tipo',
    'cnpj',
    'cnpj_origem',
    'instituicao_financeira',
    'indice',
    'qtd_total_reclamacoes',
}

BANCOS_EXPECTED_COLUMNS = {'segmento', 'cnpj', 'nome_banco', 'nome_alternativo'}


def _trusted_path(filename: str) -> str:
    return os.path.join(TRUSTED_DIR, filename)


@pytest.mark.skipif(
    not os.path.exists(_trusted_path('reclamacoes.parquet')),
    reason="Camada Trusted não gerada neste ambiente (rode a DAG antes do teste).",
)
def test_reclamacoes_tem_colunas_esperadas():
    df = pd.read_parquet(_trusted_path('reclamacoes.parquet'))
    faltantes = RECLAMACOES_EXPECTED_COLUMNS - set(df.columns)
    assert not faltantes, f"Colunas esperadas ausentes em reclamacoes: {faltantes}"


@pytest.mark.skipif(
    not os.path.exists(_trusted_path('reclamacoes.parquet')),
    reason="Camada Trusted não gerada neste ambiente (rode a DAG antes do teste).",
)
def test_reclamacoes_cnpj_origem_sempre_documentado():
    df = pd.read_parquet(_trusted_path('reclamacoes.parquet'))
    assert df['cnpj_origem'].notna().all(), (
        "Toda linha de reclamacoes deve documentar como o cnpj foi obtido "
        "(direto, fuzzy match ou 'nao encontrado')"
    )


@pytest.mark.skipif(
    not os.path.exists(_trusted_path('bancos.parquet')),
    reason="Camada Trusted não gerada neste ambiente (rode a DAG antes do teste).",
)
def test_bancos_tem_colunas_esperadas():
    df = pd.read_parquet(_trusted_path('bancos.parquet'))
    faltantes = BANCOS_EXPECTED_COLUMNS - set(df.columns)
    assert not faltantes, f"Colunas esperadas ausentes em bancos: {faltantes}"


@pytest.mark.skipif(
    not os.path.exists(_trusted_path('bancos.parquet')),
    reason="Camada Trusted não gerada neste ambiente (rode a DAG antes do teste).",
)
def test_bancos_cnpj_sem_nulos_e_unico():
    df = pd.read_parquet(_trusted_path('bancos.parquet'))
    assert df['cnpj'].notna().all(), "Coluna cnpj em bancos.parquet contém nulos"
    assert df['cnpj'].is_unique, "Coluna cnpj em bancos.parquet contém duplicados"


@pytest.mark.skipif(
    not os.path.exists(_trusted_path('empregados_glassdoor.parquet')),
    reason="Camada Trusted não gerada neste ambiente (rode a DAG antes do teste).",
)
def test_empregados_tem_employer_name_e_cnpj():
    df = pd.read_parquet(_trusted_path('empregados_glassdoor.parquet'))
    assert 'employer_name' in df.columns
    assert df['cnpj'].notna().all(), "Coluna cnpj em empregados_glassdoor.parquet contém nulos"
