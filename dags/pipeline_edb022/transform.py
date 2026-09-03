"""
Etapa 2 — Tratamento (camada Trusted).

Lê os Parquet da camada Raw e aplica, com pandas puro (sem SQL), a limpeza
e padronização de cada base — a mesma lógica do repositório
`eEDB-022-02-fuzzy-main/src/02_trusted.py`:

  * Reclamações (BACEN):
      - Corrige nomes de coluna e remove coluna fantasma do ";" final
      - "Trimestre" (ex.: "1º") -> inteiro (1..4)
      - "Índice" (ex.: "54,79") -> float (54.79), vazio -> NaN
      - Quantidades -> inteiro, vazio -> NaN
      - CNPJ -> string sem zeros à esquerda, vazio -> NaN
      - Resolve o CNPJ das linhas "Conglomerado" (grandes bancos, ex.:
        "BRADESCO (conglomerado)") casando o nome com a base de Bancos já
        tratada — primeiro por nome exato, depois (fallback) por fuzzy
        matching (RapidFuzz) para nomes com sigla/pontuação/acento
        diferentes.

  * Bancos (enquadramento/segmento):
      - Padroniza nomes, remove espaços, remove zeros à esquerda do CNPJ
      - Deduplica CNPJs que apareciam com 2 nomes (nome consolidado
        "- PRUDENCIAL" e a razão social individual), mantendo o
        "- PRUDENCIAL" como nome oficial e preservando o outro em
        "nome_alternativo"

  * Empregados (Glassdoor):
      - Une os dois arquivos de origem (match e match_less)
      - Resolve o CNPJ de cada empregador (match: por Segmento+Nome, com
        fallback fuzzy; match_less: já vem com CNPJ)
      - Deduplica por CNPJ, priorizando o registro do arquivo "match"

O resultado de cada base tratada é salvo em Parquet (Data Lake/Trusted) e,
opcionalmente, carregado no Postgres de dados (schema "trusted").
"""
import os

import pandas as pd

from .common import RAW_DIR, TRUSTED_DIR, FUZZY_SCORE_MINIMO, upload_to_s3, load_df_to_postgres
from .fuzzy_match import normalizar_nome, resolver_pendentes_por_fuzzy
from .logging_config import get_logger

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Bancos (enquadramento / segmento)
# --------------------------------------------------------------------------- #
def tratar_bancos(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Limpa e padroniza a base de enquadramento de bancos; deduplica por CNPJ."""
    df = df_raw.copy()
    df = df.rename(columns={'Segmento': 'segmento', 'CNPJ': 'cnpj', 'Nome': 'nome'})
    df['segmento'] = df['segmento'].str.strip()
    df['cnpj'] = df['cnpj'].str.strip()
    # Padroniza CNPJ removendo zeros à esquerda (mesma convenção usada nas outras bases)
    df['cnpj'] = df['cnpj'].apply(lambda v: str(int(v)) if pd.notna(v) and v != '' else v)
    df['nome'] = df['nome'].str.strip()

    df['eh_nome_prudencial'] = df['nome'].str.contains('PRUDENCIAL', case=False)
    df = df.sort_values(by=['cnpj', 'eh_nome_prudencial'], ascending=[True, False])

    alternativos = (
        df[~df['eh_nome_prudencial']]
        .drop_duplicates(subset='cnpj')
        .set_index('cnpj')['nome']
    )

    df_dedup = df.drop_duplicates(subset='cnpj', keep='first').copy()
    df_dedup['nome_alternativo'] = df_dedup['cnpj'].map(alternativos)
    df_dedup.loc[df_dedup['nome_alternativo'] == df_dedup['nome'], 'nome_alternativo'] = pd.NA

    df_dedup = df_dedup.drop(columns=['eh_nome_prudencial'])
    df_dedup = df_dedup.rename(columns={'nome': 'nome_banco'})

    return df_dedup[
        ['segmento', 'cnpj', 'nome_banco', 'nome_alternativo', 'arquivo_origem']
    ].reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Reclamações
# --------------------------------------------------------------------------- #
def tratar_reclamacoes(df_raw: pd.DataFrame, bancos: pd.DataFrame) -> pd.DataFrame:
    """Limpa, tipa e resolve o CNPJ da base de reclamações do BACEN."""
    df = df_raw.copy()

    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    df = df.rename(
        columns={
            'Ano': 'ano',
            'Trimestre': 'trimestre',
            'Categoria': 'categoria',
            'Tipo': 'tipo',
            'CNPJ IF': 'cnpj',
            'Instituição financeira': 'instituicao_financeira',
            'Índice': 'indice',
            'Quantidade de reclamações reguladas procedentes': 'qtd_reclamacoes_reguladas_procedentes',
            'Quantidade de reclamações reguladas - outras': 'qtd_reclamacoes_reguladas_outras',
            'Quantidade de reclamações não reguladas': 'qtd_reclamacoes_nao_reguladas',
            'Quantidade total de reclamações': 'qtd_total_reclamacoes',
            'Quantidade total de clientes \x96 CCS e SCR': 'qtd_total_clientes_ccs_scr',
            'Quantidade de clientes \x96 CCS': 'qtd_clientes_ccs',
            'Quantidade de clientes \x96 SCR': 'qtd_clientes_scr',
        }
    )

    for col in ['categoria', 'tipo', 'instituicao_financeira']:
        df[col] = df[col].str.strip()

    df['ano'] = pd.to_numeric(df['ano'], errors='coerce').astype('Int64')
    df['trimestre'] = df['trimestre'].str.extract(r'(\d)').astype('Int64')

    df['cnpj'] = df['cnpj'].str.strip()
    df.loc[df['cnpj'] == '', 'cnpj'] = pd.NA
    df['cnpj'] = df['cnpj'].apply(lambda v: str(int(v)) if pd.notna(v) else v)

    df['indice'] = df['indice'].str.strip().str.replace(',', '.', regex=False).replace('', pd.NA)
    df['indice'] = pd.to_numeric(df['indice'], errors='coerce')

    colunas_qtd = [
        'qtd_reclamacoes_reguladas_procedentes',
        'qtd_reclamacoes_reguladas_outras',
        'qtd_reclamacoes_nao_reguladas',
        'qtd_total_reclamacoes',
        'qtd_total_clientes_ccs_scr',
        'qtd_clientes_ccs',
        'qtd_clientes_scr',
    ]
    for col in colunas_qtd:
        df[col] = df[col].astype(str).str.strip().replace('', pd.NA)
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    df = df.drop_duplicates()

    # ----------------------------------------------------------------- #
    # Resolução do CNPJ para as linhas "Conglomerado" (grandes bancos)
    # ----------------------------------------------------------------- #
    bancos_nome = bancos[['cnpj', 'segmento', 'nome_banco', 'nome_alternativo']].copy()
    lookup_oficial = bancos_nome[['cnpj', 'segmento', 'nome_banco']].rename(columns={'nome_banco': 'nome'})
    lookup_alt = (
        bancos_nome[bancos_nome['nome_alternativo'].notna()][['cnpj', 'segmento', 'nome_alternativo']]
        .rename(columns={'nome_alternativo': 'nome'})
    )
    lookup_nomes = pd.concat([lookup_oficial, lookup_alt], ignore_index=True)
    lookup_nomes['nome_join'] = (
        lookup_nomes['nome'].str.replace(' - PRUDENCIAL', '', regex=False).str.strip().str.upper()
    )
    lookup_nomes = lookup_nomes.drop_duplicates(subset='nome_join')[['nome_join', 'cnpj', 'segmento']]

    mascara_conglomerado = df['tipo'] == 'Conglomerado'
    df['nome_join'] = pd.NA
    df.loc[mascara_conglomerado, 'nome_join'] = (
        df.loc[mascara_conglomerado, 'instituicao_financeira']
        .str.replace(r'\s*\(conglomerado\)\s*$', '', regex=True)
        .str.strip()
        .str.upper()
    )

    df = df.merge(lookup_nomes, on='nome_join', how='left', suffixes=('', '_resolvido'))

    # Fallback fuzzy para conglomerados que não bateram no cruzamento exato.
    lookup_nomes['nome_norm'] = lookup_nomes['nome_join'].apply(normalizar_nome)
    df['nome_norm'] = df['nome_join'].apply(normalizar_nome)
    mascara_pendente = mascara_conglomerado & df['cnpj_resolvido'].isna()

    df = resolver_pendentes_por_fuzzy(
        df,
        mascara_pendente=mascara_pendente,
        coluna_nome_origem='nome_norm',
        lookup=lookup_nomes,
        coluna_nome_lookup='nome_norm',
        colunas_retorno={'cnpj': 'cnpj_resolvido'},
        score_minimo=FUZZY_SCORE_MINIMO,
    )

    df['cnpj'] = df['cnpj'].fillna(df['cnpj_resolvido'])

    df['cnpj_origem'] = pd.NA
    df.loc[df['tipo'] == 'Banco/financeira', 'cnpj_origem'] = 'direto (CNPJ na origem)'
    df.loc[
        mascara_conglomerado & df['cnpj_resolvido'].notna() & df['nome_norm_fuzzy_score'].isna(),
        'cnpj_origem',
    ] = 'resolvido pelo nome do conglomerado (match exato)'
    score_numerico = pd.to_numeric(df['nome_norm_fuzzy_score'], errors='coerce').round().astype('Int64')
    df.loc[mascara_conglomerado & df['nome_norm_fuzzy_score'].notna(), 'cnpj_origem'] = (
        'resolvido pelo nome do conglomerado (fuzzy match, score=' + score_numerico.astype(str) + ')'
    )
    df.loc[mascara_conglomerado & df['cnpj_resolvido'].isna(), 'cnpj_origem'] = (
        'nao encontrado (ex.: fintechs/IPs fora do enquadramento de Bancos)'
    )

    df = df.drop(
        columns=['nome_join', 'nome_norm', 'cnpj_resolvido', 'segmento', 'nome_norm_fuzzy_candidato', 'nome_norm_fuzzy_score'],
        errors='ignore',
    )

    colunas_finais = [
        'ano',
        'trimestre',
        'categoria',
        'tipo',
        'cnpj',
        'cnpj_origem',
        'instituicao_financeira',
        'indice',
        *colunas_qtd,
        'arquivo_origem',
    ]
    return df[colunas_finais].reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Empregados (Glassdoor)
# --------------------------------------------------------------------------- #
RENOMEIA_GLASSDOOR = {
    'employer_name': 'employer_name',
    'reviews_count': 'reviews_count',
    'culture_count': 'culture_count',
    'salaries_count': 'salaries_count',
    'benefits_count': 'benefits_count',
    'employer-website': 'employer_website',
    'employer-headquarters': 'employer_headquarters',
    'employer-founded': 'employer_founded',
    'employer-industry': 'employer_industry',
    'employer-revenue': 'employer_revenue',
    'url': 'url',
    'Geral': 'nota_geral',
    'Cultura e valores': 'nota_cultura_valores',
    'Diversidade e inclusão': 'nota_diversidade_inclusao',
    'Qualidade de vida': 'nota_qualidade_vida',
    'Alta liderança': 'nota_alta_lideranca',
    'Remuneração e benefícios': 'nota_remuneracao_beneficios',
    'Oportunidades de carreira': 'nota_oportunidades_carreira',
    'Recomendam para outras pessoas(%)': 'pct_recomendam_empresa',
    'Perspectiva positiva da empresa(%)': 'pct_perspectiva_positiva',
    'match_percent': 'match_percent',
}

COLUNAS_NUMERICAS_INT = ['reviews_count', 'culture_count', 'salaries_count', 'benefits_count', 'match_percent']
COLUNAS_NUMERICAS_FLOAT = [
    'employer_founded',
    'nota_geral',
    'nota_cultura_valores',
    'nota_diversidade_inclusao',
    'nota_qualidade_vida',
    'nota_alta_lideranca',
    'nota_remuneracao_beneficios',
    'nota_oportunidades_carreira',
    'pct_recomendam_empresa',
    'pct_perspectiva_positiva',
]


def _tratar_numericos_glassdoor(df: pd.DataFrame) -> pd.DataFrame:
    for col in COLUNAS_NUMERICAS_INT:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    for col in COLUNAS_NUMERICAS_FLOAT:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    for col in ['employer_name', 'employer_website', 'employer_headquarters', 'employer_industry', 'employer_revenue', 'url']:
        df[col] = df[col].astype(str).str.strip()
    return df


def tratar_empregados(df_match_raw: pd.DataFrame, df_match_less_raw: pd.DataFrame, bancos: pd.DataFrame) -> pd.DataFrame:
    """Une as duas fontes de empregados, resolve CNPJ e deduplica por CNPJ."""
    match = df_match_raw.rename(columns=RENOMEIA_GLASSDOOR).copy()
    match['nome_join'] = match['Nome'].str.strip().str.upper()
    match['segmento'] = match['Segmento'].str.strip()
    match['origem_match'] = 'match'

    bancos_nomes = bancos[['segmento', 'cnpj', 'nome_banco']].rename(columns={'nome_banco': 'nome'})
    bancos_nomes_alt = (
        bancos[bancos['nome_alternativo'].notna()][['segmento', 'cnpj', 'nome_alternativo']]
        .rename(columns={'nome_alternativo': 'nome'})
    )
    bancos_join = pd.concat([bancos_nomes, bancos_nomes_alt], ignore_index=True)
    bancos_join['nome_join'] = (
        bancos_join['nome'].str.replace(' - PRUDENCIAL', '', regex=False).str.strip().str.upper()
    )
    bancos_join = bancos_join.drop_duplicates(subset=['segmento', 'nome_join'])

    match = match.merge(
        bancos_join[['segmento', 'nome_join', 'cnpj']],
        on=['segmento', 'nome_join'],
        how='left',
    )

    # Fallback fuzzy para nomes que não bateram exatamente, por segmento.
    match['nome_norm'] = match['nome_join'].apply(normalizar_nome)
    bancos_join['nome_norm'] = bancos_join['nome_join'].apply(normalizar_nome)

    for segmento_atual in match.loc[match['cnpj'].isna(), 'segmento'].dropna().unique():
        mascara_pendente = (match['segmento'] == segmento_atual) & match['cnpj'].isna()
        lookup_segmento = bancos_join[bancos_join['segmento'] == segmento_atual]
        match = resolver_pendentes_por_fuzzy(
            match,
            mascara_pendente=mascara_pendente,
            coluna_nome_origem='nome_norm',
            lookup=lookup_segmento,
            coluna_nome_lookup='nome_norm',
            colunas_retorno={'cnpj': 'cnpj'},
            score_minimo=FUZZY_SCORE_MINIMO,
        )

    match = match.drop(columns=['nome_norm'], errors='ignore')

    match_less = df_match_less_raw.rename(columns=RENOMEIA_GLASSDOOR).copy()
    match_less['cnpj'] = match_less['CNPJ'].str.strip()
    match_less['origem_match'] = 'match_less'
    match_less = match_less.merge(
        bancos_join[['cnpj', 'segmento']].drop_duplicates(subset='cnpj'),
        on='cnpj',
        how='left',
    )

    colunas_comuns = list(RENOMEIA_GLASSDOOR.values()) + ['cnpj', 'segmento', 'origem_match']

    empregados = pd.concat([match[colunas_comuns], match_less[colunas_comuns]], ignore_index=True)
    empregados = _tratar_numericos_glassdoor(empregados)
    empregados = empregados.drop_duplicates()

    # 1 registro por CNPJ: prioriza "match" e, dentro dela, maior match_percent.
    empregados = empregados[empregados['cnpj'].notna()]
    empregados['prioridade_origem'] = (empregados['origem_match'] == 'match_less').astype(int)
    empregados = empregados.sort_values(
        by=['cnpj', 'prioridade_origem', 'match_percent'], ascending=[True, True, False]
    )
    empregados = empregados.drop_duplicates(subset='cnpj', keep='first')
    empregados = empregados.drop(columns=['prioridade_origem'])

    return empregados.reset_index(drop=True)


# --------------------------------------------------------------------------- #
def transform_to_trusted():
    logger.info("Lendo camada Raw (Parquet)...")
    reclamacoes_raw = pd.read_parquet(os.path.join(RAW_DIR, 'reclamacoes.parquet'))
    bancos_raw = pd.read_parquet(os.path.join(RAW_DIR, 'bancos_enquadramento.parquet'))
    empregados_match_raw = pd.read_parquet(os.path.join(RAW_DIR, 'empregados_match.parquet'))
    empregados_match_less_raw = pd.read_parquet(os.path.join(RAW_DIR, 'empregados_match_less.parquet'))

    logger.info("Tratando Bancos (enquadramento)...")
    bancos = tratar_bancos(bancos_raw)
    logger.info("  -> %d linhas tratadas (deduplicadas por CNPJ)", len(bancos))

    logger.info("Tratando Reclamações...")
    reclamacoes = tratar_reclamacoes(reclamacoes_raw, bancos)
    cnpjs_resolvidos = reclamacoes['cnpj'].notna().sum()
    logger.info("  -> %d linhas tratadas", len(reclamacoes))
    logger.info("  -> CNPJ resolvido para %d de %d linhas", cnpjs_resolvidos, len(reclamacoes))
    if cnpjs_resolvidos < len(reclamacoes):
        logger.warning(
            "  -> %d linhas de reclamações ficaram sem CNPJ resolvido "
            "(esperado para fintechs/IPs fora do enquadramento de Bancos)",
            len(reclamacoes) - cnpjs_resolvidos,
        )

    logger.info("Tratando Empregados (Glassdoor)...")
    empregados = tratar_empregados(empregados_match_raw, empregados_match_less_raw, bancos)
    cnpjs_resolvidos_emp = empregados['cnpj'].notna().sum()
    logger.info("  -> %d linhas tratadas", len(empregados))
    logger.info("  -> CNPJ resolvido para %d de %d empregadores", cnpjs_resolvidos_emp, len(empregados))
    if cnpjs_resolvidos_emp < len(empregados):
        logger.warning(
            "  -> %d empregadores ficaram sem CNPJ resolvido",
            len(empregados) - cnpjs_resolvidos_emp,
        )

    logger.info("Salvando camada Trusted (Parquet)...")
    bancos_path = os.path.join(TRUSTED_DIR, 'bancos.parquet')
    recl_path = os.path.join(TRUSTED_DIR, 'reclamacoes.parquet')
    emp_path = os.path.join(TRUSTED_DIR, 'empregados_glassdoor.parquet')

    bancos.to_parquet(bancos_path, index=False)
    reclamacoes.to_parquet(recl_path, index=False)
    empregados.to_parquet(emp_path, index=False)
    logger.info("Parquet gravado: %s, %s, %s", bancos_path, recl_path, emp_path)

    upload_to_s3(bancos_path, 'Trusted/bancos.parquet')
    upload_to_s3(recl_path, 'Trusted/reclamacoes.parquet')
    upload_to_s3(emp_path, 'Trusted/empregados_glassdoor.parquet')

    load_df_to_postgres(bancos, 'bancos', schema='trusted')
    load_df_to_postgres(reclamacoes, 'reclamacoes', schema='trusted')
    load_df_to_postgres(empregados, 'empregados_glassdoor', schema='trusted')
