FROM apache/airflow:3.4.0

USER root

RUN apt-get update && apt-get install -y gcc && apt-get clean

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt