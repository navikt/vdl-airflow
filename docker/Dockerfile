FROM apache/airflow:2.6.3-python3.10
LABEL org.opencontainers.image.source https://github.com/navikt/vdl-airflow
USER airflow

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.6.2/constraints-3.10.txt"
