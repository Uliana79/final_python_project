FROM apache/airflow:2.9.3

WORKDIR /opt/airflow

COPY requirements.txt /opt/airflow/requirements.txt

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

