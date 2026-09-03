FROM apache/airflow:2.8.1

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Dependências do pipeline:
#   - great_expectations, pandas, pyarrow: tratamento e validação de dados
#   - boto3: upload opcional para S3 (UPLOAD_TO_S3=true)
#   - sqlalchemy + psycopg2-binary: carga opcional no Postgres de dados
#     (LOAD_TO_POSTGRES=true), schemas raw/trusted/delivery
#   - rapidfuzz: fuzzy matching de nomes para resolver CNPJ (ver
#     dags/pipeline_edb022/fuzzy_match.py), do repositório eEDB-022-02-fuzzy-main
#   - acryl-datahub: emissor de metadados (schema/descrição de cada
#     dataset) para o DataHub via API REST (ver dags/pipeline_edb022/metadata.py).
#     Instalamos só o core (sem o extra "[airflow]", que traz o plugin de
#     lineage automático e é bem mais pesado) — suficiente para catalogar
#     as camadas Raw/Trusted/Delivery, que é o que a Atividade 5 pede.
RUN pip install --no-cache-dir \
    great_expectations==0.18.19 \
    pandas \
    pyarrow \
    boto3 \
    sqlalchemy \
    psycopg2-binary \
    rapidfuzz \
    acryl-datahub \
    pytest
