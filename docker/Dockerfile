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

# Airflow itself comes from the base image; pin extras with the official constraints.
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
      ".[aws,gcp,local]" pyspark==3.5.3

ENV PYTHONPATH=/opt/pipeline:/opt/pipeline/airflow/plugins \
    PIPELINE_CONFIG_PATH=/opt/pipeline/config/pipeline_config.yaml \
    PYSPARK_PYTHON=python
