FROM apache/airflow:2.9.3

WORKDIR /opt/airflow

COPY requirements.txt /opt/airflow/requirements.txt

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/airflow/jars
RUN curl -o /opt/airflow/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
