"""
FUZZY MATCHING DE NOMES (fallback para o cruzamento por nome)
================================================================
Usado como segunda tentativa quando o cruzamento exato por nome
normalizado (upper + strip) não encontra correspondência -- por
exemplo, nomes com sigla/abreviação diferente, pontuação (S.A., LTDA),
acentuação ou ordem de palavras diferente entre as bases (Reclamações
BACEN, Bancos/enquadramento e Empregados/Glassdoor).

Motor usado: RapidFuzz (rapidfuzz.process + fuzz.token_sort_ratio),
que tolera palavras fora de ordem (ex.: "UNIBANCO ITAU" casa com
"ITAU UNIBANCO").
"""
import re                     # Para expressões regulares (limpeza de texto)
import unicodedata            # Para remover acentos (normalização Unicode)

import pandas as pd           # Para manipulação de DataFrames
from rapidfuzz import fuzz, process   # Motor de fuzzy matching

from .logging_config import get_logger

logger = get_logger(__name__)

# Compila uma expressão regular para remover sufixos/termos societários
# que atrapalham a comparação (ex.: "ITAU S.A." vs "ITAU LTDA").
# O padrão busca palavras como S/A, LTDA, BANCO, etc. (com limites de palavra \b)
SUFIXOS_EMPRESARIAIS = re.compile(
    r"\b(S\s*A|S\s*A\s*S|LTDA|LIMITADA|BANCO|BANCOS|BCO|BCOS|FINANCEIRA|"
    r"CONGLOMERADO|PRUDENCIAL|GRUPO|HOLDING|INSTITUICAO|INSTITUICOES)\b"
)


def normalizar_nome(nome) -> str:
    """
    Normaliza um nome de instituição para comparação fuzzy:
    - Converte para maiúsculas
    - Remove acentos (usando normalização Unicode)
    - Remove pontuação (mantém apenas letras e espaços)
    - Remove sufixos societários comuns (S/A, LTDA, etc.)
    - Remove espaços extras
    Retorna string vazia para valores nulos.
    """
    # Se o valor for nulo (None ou NaN), retorna string vazia
    if nome is None or (isinstance(nome, float) and pd.isna(nome)):
        return ""
    # Converte para string, remove espaços das bordas e coloca em maiúsculas
    texto = str(nome).upper().strip()
    # Normaliza Unicode: decompõe caracteres acentuados (ex.: 'ç' -> 'c' + '̧')
    # Depois codifica para ASCII ignorando caracteres não suportados (remove acentos)
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    # Substitui qualquer caractere que não seja letra, número ou espaço por espaço
    texto = re.sub(r"[^\w\s]", " ", texto)
    # Remove os sufixos empresariais (substitui por espaço)
    texto = SUFIXOS_EMPRESARIAIS.sub(" ", texto)
    # Substitui múltiplos espaços por um único e remove espaços das bordas
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto


def melhor_correspondencia(nome_norm: str, candidatos: list, score_minimo: int = 88):
    """
    Retorna (candidato, score 0-100) com a melhor correspondência
    aproximada de `nome_norm` em `candidatos`, ou (None, 0) se nada
    atingir `score_minimo`.
    Usa `token_sort_ratio` que ignora a ordem das palavras, útil para
    nomes de bancos/empresas (ex.: "UNIBANCO ITAU" vs "ITAU UNIBANCO").
    """
    # Se não houver nome ou lista vazia, retorna sem correspondência
    if not nome_norm or not candidatos:
        return None, 0
    # Usa process.extractOne para encontrar o melhor candidato acima do score mínimo
    resultado = process.extractOne(
        nome_norm, candidatos, scorer=fuzz.token_sort_ratio, score_cutoff=score_minimo
    )
    # Se não encontrou nenhum acima do limite, retorna None
    if resultado is None:
        return None, 0
    # Desempacota: candidato, score, índice (que ignoramos)
    candidato, score, _ = resultado
    return candidato, score


def resolver_pendentes_por_fuzzy(
    df: pd.DataFrame,
    mascara_pendente: pd.Series,
    coluna_nome_origem: str,
    lookup: pd.DataFrame,
    coluna_nome_lookup: str,
    colunas_retorno: dict,
    score_minimo: int = 88,
) -> pd.DataFrame:
    """
    Preenche, apenas nas linhas de `df` marcadas por `mascara_pendente`,
    as colunas listadas em `colunas_retorno` usando fuzzy match do nome
    normalizado (coluna `coluna_nome_origem`) contra `lookup[coluna_nome_lookup]`
    (que já deve estar normalizado com `normalizar_nome`).

    Parâmetros:
      - df: DataFrame original
      - mascara_pendente: Series booleana indicando quais linhas precisam de resolução
      - coluna_nome_origem: nome da coluna em df que contém o nome normalizado
      - lookup: DataFrame de referência (com nomes e valores a serem retornados)
      - coluna_nome_lookup: nome da coluna em lookup com os nomes normalizados
      - colunas_retorno: dict {coluna_no_lookup: coluna_no_df_destino}
          ex.: {"cnpj": "cnpj_resolvido"}
      - score_minimo: pontuação mínima (0-100) para aceitar uma correspondência

    Adiciona duas colunas de auditoria (só nas linhas resolvidas por fuzzy):
      - "<coluna_nome_origem>_fuzzy_candidato": nome normalizado que casou
      - "<coluna_nome_origem>_fuzzy_score": score da correspondência

    Retorna o DataFrame modificado (cópia).
    """
    total_pendente = int(mascara_pendente.sum())
    logger.info(
        "Fuzzy match: tentando resolver %d linha(s) pendente(s) contra %d candidato(s) (score mínimo=%d)",
        total_pendente, lookup[coluna_nome_lookup].nunique(), score_minimo,
    )

    # Cria uma cópia para evitar alterar o original
    df = df.copy()
    # Nomes das colunas de auditoria
    col_candidato = f"{coluna_nome_origem}_fuzzy_candidato"
    col_score = f"{coluna_nome_origem}_fuzzy_score"
    # Inicializa as colunas de auditoria com NA (se não existirem)
    if col_candidato not in df.columns:
        df[col_candidato] = pd.NA
    if col_score not in df.columns:
        df[col_score] = pd.NA

    # Lista única de todos os nomes normalizados disponíveis no lookup
    candidatos = lookup[coluna_nome_lookup].dropna().unique().tolist()
    # Cria um índice para acesso rápido: para cada nome normalizado, guarda a linha correspondente
    lookup_indexado = lookup.drop_duplicates(subset=coluna_nome_lookup).set_index(coluna_nome_lookup)

    # Itera sobre os índices das linhas pendentes (mascara_pendente == True)
    total_resolvido = 0
    for idx in df.index[mascara_pendente]:
        # Obtém o nome normalizado da linha atual
        nome_norm = df.at[idx, coluna_nome_origem]
        # Busca a melhor correspondência entre esse nome e os candidatos
        candidato, score = melhor_correspondencia(nome_norm, candidatos, score_minimo)
        # Se não encontrou (candidato None), pula para a próxima linha
        if candidato is None:
            logger.debug("Fuzzy match: nenhuma correspondência para '%s' (>= %d)", nome_norm, score_minimo)
            continue
        # Obtém a linha do lookup que corresponde ao candidato encontrado
        linha = lookup_indexado.loc[candidato]
        # Para cada coluna definida em colunas_retorno, preenche a coluna destino com o valor do lookup
        for col_lookup, col_destino in colunas_retorno.items():
            df.at[idx, col_destino] = linha[col_lookup]
        # Registra o candidato e o score nas colunas de auditoria
        df.at[idx, col_candidato] = candidato
        df.at[idx, col_score] = score
        total_resolvido += 1
        logger.debug("Fuzzy match: '%s' -> '%s' (score=%d)", nome_norm, candidato, score)

    logger.info(
        "Fuzzy match: %d de %d linha(s) pendente(s) resolvida(s) por aproximação.",
        total_resolvido, total_pendente,
    )
    return df
