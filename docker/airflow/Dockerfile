FROM apache/airflow:2.7.3-python3.10
LABEL org.opencontainers.image.source https://github.com/navikt/vdl-airflow
USER ${AIRFLOW_UID}

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.10.txt"
