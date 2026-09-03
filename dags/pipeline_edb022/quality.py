"""
Etapa 3 — Validação de qualidade de dados (Great Expectations) sobre a
camada Trusted, antes da consolidação na camada Delivery/Refined.

Expectations cobertas (6 no total) — adaptadas às colunas produzidas pelo
tratamento robusto (transform.py), que resolve CNPJ por nome/fuzzy match
em vez de assumir que ele já vem preenchido em todas as linhas:

  1. bancos.cnpj                não nulo
  2. bancos.cnpj                único (chave de junção — 1 linha por banco)
  3. empregados_glassdoor.employer_name  não nulo
  4. empregados_glassdoor.cnpj  não nulo (chave usada no join da Delivery)
  5. reclamacoes.cnpj_origem    não nulo (todo registro documenta como o
                                  CNPJ foi obtido, mesmo quando não foi
                                  encontrado — ver transform.py)
  6. reclamacoes                tabela não vazia (sanity check de volume)

Não exigimos mais reclamacoes.cnpj não nulo: por desenho, fintechs/IPs
fora do enquadramento de Bancos (ex.: Nubank, Stone) ficam de propósito
sem CNPJ resolvido na Trusted (documentado em cnpj_origem) e só saem da
Delivery — não é uma falha de qualidade.
"""
import os
import pandas as pd
import great_expectations as ge

from .common import TRUSTED_DIR
from .logging_config import get_logger

logger = get_logger(__name__)


class DataQualityError(ValueError):
    """Erro específico para falhas de qualidade de dados, mais fácil de filtrar nos logs."""


def _check(result, nome_check: str, mensagem_falha: str):
    """Valida o resultado de uma expectation do Great Expectations.

    - nome_check: descrição curta do que está sendo validado (logada em
      caso de sucesso), ex.: "Bancos.cnpj não nulo".
    - mensagem_falha: descrição do problema, usada apenas se a expectation
      falhar (logada como erro e incluída na exceção).
    """
    if not result['success']:
        logger.error("Data Quality Check Failed [%s]: %s", nome_check, mensagem_falha)
        raise DataQualityError(f"Data Quality Check Failed: {mensagem_falha}")
    logger.info("Check OK: %s", nome_check)


def data_quality_checks():
    logger.info("Carregando camada Trusted para validação (Great Expectations)...")
    bancos_ge = ge.dataset.PandasDataset(
        pd.read_parquet(os.path.join(TRUSTED_DIR, 'bancos.parquet'))
    )
    emp_ge = ge.dataset.PandasDataset(
        pd.read_parquet(os.path.join(TRUSTED_DIR, 'empregados_glassdoor.parquet'))
    )
    recl_ge = ge.dataset.PandasDataset(
        pd.read_parquet(os.path.join(TRUSTED_DIR, 'reclamacoes.parquet'))
    )

    # 1. Bancos: CNPJ não pode ser nulo (chave de junção com Reclamações/Empregados)
    _check(
        bancos_ge.expect_column_values_to_not_be_null('cnpj'),
        "Bancos.cnpj não nulo",
        "Bancos.cnpj possui valores nulos",
    )

    # 2. Bancos: CNPJ deve ser único (1 linha por banco, após deduplicação)
    _check(
        bancos_ge.expect_column_values_to_be_unique('cnpj'),
        "Bancos.cnpj único",
        "Bancos.cnpj possui valores duplicados",
    )

    # 3. Empregados: employer_name não pode ser nulo
    _check(
        emp_ge.expect_column_values_to_not_be_null('employer_name'),
        "Empregados.employer_name não nulo",
        "Empregados.employer_name possui valores nulos",
    )

    # 4. Empregados: CNPJ não pode ser nulo (linhas sem CNPJ já são descartadas em transform.py)
    _check(
        emp_ge.expect_column_values_to_not_be_null('cnpj'),
        "Empregados.cnpj não nulo",
        "Empregados.cnpj possui valores nulos",
    )

    # 5. Reclamações: cnpj_origem sempre documentado (direto, fuzzy match ou "não encontrado")
    _check(
        recl_ge.expect_column_values_to_not_be_null('cnpj_origem'),
        "Reclamacoes.cnpj_origem documentado",
        "Reclamacoes.cnpj_origem possui valores nulos — resolução de CNPJ incompleta",
    )

    # 6. Reclamações: sanity check de volume — tabela não pode estar vazia
    _check(
        recl_ge.expect_table_row_count_to_be_between(min_value=1, max_value=None),
        "Reclamacoes não vazia",
        "Reclamacoes está vazia — possível falha na extração",
    )

    logger.info("Data quality checks passed successfully! (6 expectations)")
