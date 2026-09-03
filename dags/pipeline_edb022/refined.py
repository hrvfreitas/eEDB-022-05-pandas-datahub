"""
Etapa 4 — União e entrega (camada Refined/Delivery).

Lê as três bases já tratadas na camada Trusted e as une (com pandas, sem
SQL) em uma única tabela final, na granularidade "banco x trimestre" —
mesma lógica do repositório `eEDB-022-02-fuzzy-main/src/03_delivery.py`:

    Reclamações (linhas com CNPJ resolvido)
        LEFT JOIN Bancos (segmento oficial / nome oficial)        -- por CNPJ
        LEFT JOIN Empregados Glassdoor (avaliações de funcionários) -- por CNPJ

Só entram na tabela final as linhas de Reclamações cujo CNPJ pôde ser
resolvido (direto na origem ou pelo nome do conglomerado, inclusive via
fuzzy match — ver transform.py). Fintechs/instituições de pagamento fora
do enquadramento de Bancos (ex.: Nubank, Stone, Inter) ficam de fora da
Delivery, mas continuam disponíveis na camada Trusted.
"""
import os

import pandas as pd

from .common import TRUSTED_DIR, REFINED_DIR, upload_to_s3, load_df_to_postgres
from .logging_config import get_logger

logger = get_logger(__name__)

DELIVERY_FILENAME = 'delivery_reclamacoes_bancos_funcionarios.parquet'


def montar_delivery(reclamacoes: pd.DataFrame, bancos: pd.DataFrame, empregados: pd.DataFrame) -> pd.DataFrame:
    base = reclamacoes[reclamacoes['cnpj'].notna()].copy()

    base = base.merge(
        bancos[['cnpj', 'segmento', 'nome_banco', 'nome_alternativo']],
        on='cnpj',
        how='left',
        suffixes=('', '_bancos'),
    )

    colunas_glassdoor = [c for c in empregados.columns if c not in ('cnpj', 'segmento', 'origem_match')]
    base = base.merge(
        empregados[['cnpj', 'origem_match'] + colunas_glassdoor],
        on='cnpj',
        how='left',
        suffixes=('', '_glassdoor'),
    )

    base = base.rename(
        columns={
            'segmento': 'segmento_bacen',
            'origem_match': 'origem_match_glassdoor',
        }
    )

    base['possui_avaliacao_glassdoor'] = base['employer_name'].notna()

    colunas_ordem = [
        'ano',
        'trimestre',
        'cnpj',
        'cnpj_origem',
        'instituicao_financeira',
        'nome_banco',
        'nome_alternativo',
        'segmento_bacen',
        'categoria',
        'indice',
        'qtd_reclamacoes_reguladas_procedentes',
        'qtd_reclamacoes_reguladas_outras',
        'qtd_reclamacoes_nao_reguladas',
        'qtd_total_reclamacoes',
        'qtd_total_clientes_ccs_scr',
        'qtd_clientes_ccs',
        'qtd_clientes_scr',
        'possui_avaliacao_glassdoor',
        'employer_name',
        'reviews_count',
        'culture_count',
        'salaries_count',
        'benefits_count',
        'employer_website',
        'employer_headquarters',
        'employer_founded',
        'employer_industry',
        'employer_revenue',
        'url',
        'nota_geral',
        'nota_cultura_valores',
        'nota_diversidade_inclusao',
        'nota_qualidade_vida',
        'nota_alta_lideranca',
        'nota_remuneracao_beneficios',
        'nota_oportunidades_carreira',
        'pct_recomendam_empresa',
        'pct_perspectiva_positiva',
        'match_percent',
        'origem_match_glassdoor',
    ]

    delivery = base[colunas_ordem].sort_values(by=['ano', 'trimestre', 'instituicao_financeira']).reset_index(drop=True)
    return delivery


def build_refined_layer():
    logger.info("Lendo camada Trusted (Parquet)...")
    reclamacoes = pd.read_parquet(os.path.join(TRUSTED_DIR, 'reclamacoes.parquet'))
    bancos = pd.read_parquet(os.path.join(TRUSTED_DIR, 'bancos.parquet'))
    empregados = pd.read_parquet(os.path.join(TRUSTED_DIR, 'empregados_glassdoor.parquet'))

    logger.info("Unindo as bases (Reclamações + Bancos + Empregados Glassdoor) via CNPJ...")
    delivery = montar_delivery(reclamacoes, bancos, empregados)
    logger.info("  -> tabela final com %d linhas e %d colunas", len(delivery), len(delivery.columns))
    logger.info(
        "  -> %d linhas possuem avaliação Glassdoor associada",
        delivery['possui_avaliacao_glassdoor'].sum(),
    )

    final_path = os.path.join(REFINED_DIR, DELIVERY_FILENAME)
    delivery.to_parquet(final_path, index=False)
    logger.info("Parquet gravado: %s", final_path)
    upload_to_s3(final_path, f'Refined/{DELIVERY_FILENAME}')
    load_df_to_postgres(delivery, 'tb_reclamacoes_bancos_funcionarios', schema='delivery')
    logger.info("Camada Delivery/Refined construída com sucesso.")
