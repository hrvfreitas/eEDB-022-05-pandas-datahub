# 🚀 Pipeline EDB022 — Ingestão + Tratamento de Dados (versão local, via Docker)

Este repositório junta  dois projetos do EDB022 numa única versão, com o
tratamento de dados mais robusto entre eles, pronta para rodar
**inteiramente na sua máquina** via Docker — sem depender de AWS (EC2/S3),
Airflow gerenciado ou DataHub em nuvem:



- **`edb022-ingestao-dados-aws-main`**: projeto original, com os dados
  brutos e a infraestrutura pensada para EC2 + upload para o S3.



- **`eEDB-022-02-fuzzy-main`**: reimplementação do tratamento de dados,
  bem mais completa — encoding/delimitador corretos por fonte, resolução
  de CNPJ por nome (com fallback de *fuzzy matching*, RapidFuzz),
  deduplicação de bancos e de empregadores, e carga num PostgreSQL
  relacional (schemas raw/trusted/delivery). **É essa lógica de
  tratamento que está rodando aqui**, orquestrada pelas mesmas 4 tasks do
  Airflow (em vez do `run_all.py` do repositório original).

## 🧱 O que mudou em cada junção

| Aspecto                        | Original (AWS)                                            | + Fuzzy (esta versão)                                                                                                                                 |
| ------------------------------ | --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| Executor do Airflow            | CeleryExecutor                                            | **LocalExecutor** (1 processo, sem Redis)                                                                                                             |
| Armazenamento                  | Upload obrigatório para o S3                              | Parquet local (padrão) + **Postgres opcional**; S3 opcional                                                                                           |
| Leitura das fontes             | `pd.read_csv` inferindo tipos, mesmo encoding para tudo   | **encoding/delimitador corretos por fonte** (Reclamações `;`/Latin-1, Bancos `\t`/Latin-1, Empregados `\|`/UTF-8), tudo lido como texto puro na Raw   |
| CNPJ                           | Bancos virava número (perdia dígitos ao ler TSV como int) | **CNPJ tratado como string desde a Raw**; resolvido por nome para conglomerados/empregadores, com fallback de fuzzy matching (RapidFuzz)              |
| Bancos duplicados              | Não tratado                                               | **Deduplicado por CNPJ** (nome "- PRUDENCIAL" como oficial, outro como alternativo)                                                                   |
| Qualidade (Great Expectations) | 2 checks básicos                                          | **6 checks** adaptados às novas colunas (unicidade de CNPJ em Bancos, CNPJ resolvido em Empregados, rastreabilidade via `cnpj_origem` em Reclamações) |
| Código da DAG                  | monolítico                                                | mesma estrutura de módulos, lógica de tratamento trocada                                                                                              |
| Testes                         | script sem asserts                                        | pytest com as colunas/regras da Trusted atual                                                                                                         |
| Metadados                      | DataHub, em EC2, lendo schema do S3                       | **DataHub**, rodando local (`datahub docker quickstart`), cataloga schema de Raw/Trusted/Delivery via task `emit_metadata`                            |

A arquitetura de camadas é a mesma dos dois projetos — **Raw → Trusted →
Delivery/Refined**.

## 📂 Estrutura

```
.
├── Dockerfile                  # imagem do Airflow com as libs do pipeline
├── docker-compose.yaml         # Airflow (LocalExecutor) + Postgres de metadados + Postgres de dados
├── .env.example                # variáveis de ambiente (copiar para .env)
├── Dados/                      # dados brutos de entrada (Bancos, Empregados, Reclamacoes)
├── dags/
│   ├── .airflowignore          # impede o Airflow de escanear pipeline_edb022/ como DAGs
│   ├── ingestao_dados_dag.py   # define a DAG (só orquestração)
│   └── pipeline_edb022/
│       ├── common.py           # paths, flags de S3/Postgres (ambos opcionais)
│       ├── db.py                # conexão com o Postgres de dados (schemas raw/trusted/delivery)
│       ├── fuzzy_match.py       # normalização de nomes + fuzzy matching (RapidFuzz)
│       ├── extract.py          # etapa 1: fontes -> Raw (encoding/delimitador corretos)
│       ├── transform.py        # etapa 2: Raw -> Trusted (limpeza, tipagem, resolução de CNPJ)
│       ├── quality.py          # etapa 3: Great Expectations
│       ├── refined.py          # etapa 4: Trusted -> Delivery/Refined (join final por CNPJ)
│       └── metadata.py         # etapa 5: catalogação no DataHub (schema de cada dataset)
└── tests/
    └── test_trusted_schema.py  # testes de schema (pytest)
```

## ▶️ Como executar

### Pré-requisitos

- Docker e Docker Compose instalados.
- Pelo menos 4 GB de RAM livres para o Docker.

### 1. Configurar variáveis de ambiente

```bash
cp .env.example .env
# Linux/macOS: ajuste o AIRFLOW_UID com o seu UID real
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 2. Inicializar o banco do Airflow (primeira vez)

```bash
docker compose up airflow-init
```

### 3. Subir os containers

```bash
docker compose up -d
```

Isso sobe: Postgres de metadados do Airflow, **Postgres de dados**
(`postgres-dw`, schemas raw/trusted/delivery, exposto em `localhost:5433`),
o webserver e o scheduler do Airflow.

### 4. Acessar a interface

Abra **http://localhost:8081** (usuário/senha: `airflow` / `airflow`) e
ative a DAG `ingestao_dados_edb022`. Ela executa, em sequência:

1. **`extract_and_load_raw`** — lê Reclamações (`;`, Latin-1), Bancos
   (`\t`, Latin-1) e Empregados (`|`, UTF-8) com o encoding/delimitador
   corretos de cada um, tudo como texto puro, e grava em
   `Dados/DataLake/Raw/*.parquet` (+ Postgres, schema `raw`).
2. **`transform_to_trusted`** — limpa e tipa cada base; resolve o CNPJ das
   linhas "Conglomerado" (grandes bancos) e dos empregadores Glassdoor
   casando pelo nome (com *fuzzy matching* como fallback para nomes com
   sigla/pontuação/acento diferentes); deduplica bancos e empregadores por
   CNPJ. Grava em `Dados/DataLake/Trusted/*.parquet` (+ Postgres, schema
   `trusted`).
3. **`data_quality_checks`** — 6 expectations do Great Expectations sobre
   a Trusted (CNPJ único em Bancos, CNPJ resolvido em Empregados,
   `cnpj_origem` sempre documentado em Reclamações, tabela não vazia,
   etc.).
4. **`build_refined_layer`** — une Reclamações + Bancos + Empregados por
   CNPJ, gera a tabela final (~738 linhas × 40 colunas) em
   `Dados/DataLake/Refined/delivery_reclamacoes_bancos_funcionarios.parquet`
   (+ Postgres, schema `delivery`, tabela `tb_reclamacoes_bancos_funcionarios`).
5. **`emit_metadata`** — cataloga o schema (colunas/tipos) e uma breve
   descrição de cada dataset (Raw/Trusted/Delivery) no **DataHub**, via API
   REST. Se o DataHub não estiver rodando, esta etapa só avisa nos logs e
   segue — não falha o pipeline (ver seção "DataHub" abaixo).

Todos os arquivos gerados ficam disponíveis diretamente na pasta
`Dados/DataLake/` do seu host (volume montado).

### 5. Consultar o Postgres de dados (opcional)

```bash
psql -h localhost -p 5433 -U postgres -d case_dados
\dt raw.*
\dt trusted.*
\dt delivery.*
select * from delivery.tb_reclamacoes_bancos_funcionarios limit 10;
```

### 6. Rodar os testes (opcional)

Depois de a DAG rodar pelo menos uma vez (para gerar a camada Trusted):

```bash
docker compose run --rm --entrypoint bash airflow-cli -c "pytest /opt/airflow/tests/test_trusted_schema.py"
```

ou, direto no host, se tiver Python com `pandas`/`pytest` instalados:

```bash
pip install pandas pytest
EDB022_TRUSTED_DIR=./Dados/DataLake/Trusted pytest tests/
```

## 🔍 Principais decisões de tratamento (herdadas do `eEDB-022-02-fuzzy-main`)

1. **Encoding por fonte**: Reclamações usa `;` e Latin-1; Bancos usa `\t`
   (TSV) e Latin-1; Empregados usa `|` e UTF-8. Ler tudo com o mesmo
   encoding (como a versão anterior fazia) corrompe os acentos.
2. **Chave de integração = CNPJ (raiz, sem zeros à esquerda)**. Reclamações
   guarda o CNPJ com zero-padding; Bancos e Empregados não. Normalizado
   antes do cruzamento.
3. **CNPJ dos grandes bancos ("Conglomerado")**: nas Reclamações, vêm sem
   CNPJ (ex.: `"BRADESCO (conglomerado)"`). Resolvido pelo nome, casando
   com o nome oficial em Bancos — sem essa etapa, os bancos mais
   relevantes ficariam fora do cruzamento final.
4. **Deduplicação de Bancos por CNPJ**: 15 CNPJs apareciam com dois nomes
   (consolidado "- PRUDENCIAL" e razão social individual); mantido o
   "- PRUDENCIAL" como nome oficial, o outro em `nome_alternativo`.
5. **Fallback de fuzzy matching** (RapidFuzz, `token_sort_ratio`) quando o
   cruzamento exato por nome não encontra correspondência (sigla,
   pontuação, acento ou ordem de palavras diferente). Só aceita
   automaticamente com score ≥ 83 (configurável via `FUZZY_SCORE_MINIMO`);
   casos resolvidos assim ficam marcados em `cnpj_origem` como "fuzzy
   match, score=N", para auditoria.
6. Fintechs/instituições de pagamento fora do enquadramento de Bancos
   (Nubank, Stone, Inter, C6...) permanecem sem CNPJ resolvido e por isso
   não entram na Delivery — mas continuam disponíveis, intactas, na
   Trusted.

## ☁️ Reativando o upload para o S3 (opcional)

Esta versão local não depende de AWS. Para reativar o upload:

```
UPLOAD_TO_S3=true
S3_BUCKET=<seu-bucket>
AWS_REGION=<sua-regiao>
```

e monte um arquivo de credenciais AWS válido em `./credentials/credentials`.

## 🐘 Desligando o Postgres de dados (opcional)

Por padrão a carga no Postgres (`postgres-dw`) fica ligada. Para rodar só
com Parquet (mais leve), defina no `.env`:

```
LOAD_TO_POSTGRES=false
```

O container `postgres-dw` continua subindo (é uma dependência do
`docker-compose`), mas nenhuma tabela é gravada nele.

## 🧭 DataHub (catálogo de metadados)

A **Atividade 5** pede uma ferramenta de metadados (DataHub, OpenMetadata
ou Amundsen) — este projeto usa **DataHub**, do mesmo jeito que o
repositório original (`edb022-ingestao-dados-aws-main`) já previa.

O DataHub não entra dentro deste `docker-compose.yaml` porque o stack
oficial é pesado (~10 containers: Kafka, Elasticsearch, MySQL, GMS,
frontend...). Em vez disso, ele roda **à parte**, no host, e o Airflow só
**emite metadados** para ele via API REST (task `emit_metadata`, ver
`dags/pipeline_edb022/metadata.py`) — exatamente como o projeto original
fazia.

### Como subir o DataHub e ver os metadados

```bash
# 1) instalar o CLI (uma vez só; usa Docker por baixo)
pip install acryl-datahub

# 2) subir o stack completo do DataHub (leva alguns minutos na primeira vez)
datahub docker quickstart

# 3) acessar a interface
# http://localhost:9002  (usuário/senha: datahub / datahub)
```

Com o DataHub no ar, basta rodar a DAG normalmente (`docker compose up
-d` + ativar `ingestao_dados_edb022`). A task `emit_metadata` (a última da
DAG) cataloga automaticamente:

- `edb022.raw.*` — schema bruto de cada fonte (Reclamações, Bancos,
  Empregados)
- `edb022.trusted.*` — schema tratado (com os tipos e colunas corretas,
  incluindo `cnpj_origem`)
- `edb022.delivery.tb_reclamacoes_bancos_funcionarios` — schema da tabela
  final, com contagem de linhas/colunas nas propriedades do dataset

Depois é só buscar por "edb022" na busca do DataHub para ver os datasets
catalogados, com schema e descrição de cada camada.

### Desligando a emissão de metadados (opcional)

Se não quiser subir o DataHub, defina no `.env`:

```
EMIT_METADATA_TO_DATAHUB=false
```

A task `emit_metadata` roda, avisa que está desligada, e não faz nada —
o restante do pipeline (Raw/Trusted/Delivery) não depende disso.

### Ajustando o endereço do DataHub

Por padrão, `DATAHUB_GMS_URL=http://host.docker.internal:8080` — o
container do Airflow acessa o DataHub rodando no host através desse nome
(mapeado no `docker-compose.yaml` via `extra_hosts`, necessário no
Linux). Se o seu DataHub estiver em outra máquina/porta, ajuste essa
variável no `.env`.

## 🩹 Troubleshooting

### "DAG File Processing Stats" mostra erro em `pipeline_edb022/db.py` (ou outro módulo)

O Airflow varre **recursivamente todo `.py`** dentro de `dags/` procurando
objetos `DAG` — inclusive os módulos auxiliares dentro de
`dags/pipeline_edb022/` (`db.py`, `common.py`, `extract.py`, etc.), que não
são DAGs, só código importado pela DAG. Como esses módulos usam import
relativo (`from .common import ...`), quando o Airflow tenta importá-los
como script solto durante essa varredura, o import relativo quebra com
`ImportError: attempted relative import with no known parent package` —
é esse o "# Errors 1" que aparece na tabela de stats.

**Correção:** o arquivo `dags/.airflowignore` (já incluído neste
repositório) diz ao Airflow para não escanear a pasta `pipeline_edb022/`
em busca de DAGs:

```
^pipeline_edb022/
```

Se o erro persistir depois de atualizar os arquivos, reinicie o
scheduler/webserver para ele reprocessar a pasta:

```bash
docker compose restart airflow-scheduler airflow-webserver
# ou, se preferir recriar os containers:
docker compose down && docker compose up -d
```

Isso não afeta a lógica da DAG nem os dados gerados — é só sobre como o
Airflow decide quais arquivos `.py` tentar carregar como DAG.

### `docker compose run --rm airflow-cli pytest ...` dá erro `airflow command error: argument GROUP_OR_COMMAND: invalid choice: 'pytest'`

Ao rodar `docker compose run <service> <algo>`, o Compose **substitui** o
`command` configurado no serviço pelos argumentos passados (`<algo>`), mas
**mantém o `entrypoint`**. O entrypoint padrão da imagem do Airflow, ao
receber um comando que não reconhece (como `pytest`), assume que é um
subcomando do `airflow` e tenta rodar `airflow pytest ...` — que não
existe, daí o erro.

**Correção:** force o entrypoint para `bash` explicitamente com a flag
`--entrypoint`, contornando esse comportamento:

```bash
docker compose run --rm --entrypoint bash airflow-cli -c "pytest /opt/airflow/tests/test_trusted_schema.py"
```

O mesmo vale para qualquer outro comando avulso que não seja `airflow ...`
dentro do container (`ls`, `python -c ...` etc.).

## 👥 Créditos

Baseado nos projetos **EDB022 - Engenharia de Dados** e
**eEDB-022-02-fuzzy** (Setembro de 2026), de Antonio Daniel de Souza
Linhares, Yuri Alexandre Barbosa Rodrigues e Hercules Ramos Veloso de
Freitas.
