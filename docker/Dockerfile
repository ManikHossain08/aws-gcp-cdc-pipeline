# Airflow 2.10 image with Java (local PySpark), the pipeline package and its dependencies.
# Used by docker/docker-compose.yml for a fully containerised local run.
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless zip \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
WORKDIR /opt/pipeline
COPY --chown=airflow:root pyproject.toml README.md ./
COPY --chown=airflow:root cdc_common ./cdc_common
COPY --chown=airflow:root glue ./glue
COPY --chown=airflow:root transfer ./transfer
COPY --chown=airflow:root bigquery ./bigquery

# Airflow comes from the base image. Cloud SDKs follow the Airflow constraints; the local
# stand-ins are installed afterwards without them (the constraints pin duckdb < 1.4, which
# lacks MERGE INTO).
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
      ".[aws,gcp]" pyspark==3.5.4 \
 && pip install --no-cache-dir "duckdb>=1.4" pyarrow "psycopg[binary]>=3.2"

ENV PYTHONPATH=/opt/pipeline:/opt/pipeline/airflow/plugins \
    PIPELINE_CONFIG_PATH=/opt/pipeline/config/pipeline_config.yaml \
    PYSPARK_PYTHON=python
