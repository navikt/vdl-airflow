import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.dates import days_ago
from kubernetes import client as k8s

from custom.operators.slack_operator import slack_success, test_slack

DBT_IMAGE = "ghcr.io/dbt-labs/dbt-snowflake:1.8.3@sha256:b95cc0481ec39cb48f09d63ae0f912033b10b32f3a93893a385262f4ba043f50"
SNOW_ALLOWLIST = [
    "wx23413.europe-west4.gcp.snowflakecomputing.com",
    "ocsp.snowflakecomputing.com",
    "ocsp.digicert.com:80",
    "o.pki.goog:80",
    "ocsp.pki.goo:80",
    "storage.googleapis.com",
]
BRANCH = Variable.get("FAKTURA_BRANCH")


def run_dbt_job(job_name: str):
    from dataverk_airflow import kubernetes_operator

    return kubernetes_operator(
        dag=dag,
        name=job_name,
        repo="navikt/vdl-faktura",
        branch=BRANCH,
        working_dir="dbt",
        cmds=[
            "dbt deps",
            "dbt run -s int_leverandor_oppfolging__varm",
        ],
        image=DBT_IMAGE,
        extra_envs={
            "FAKTURA_DB": Variable.get("FAKTURA_DB"),
            "DBT_USR": Variable.get("SRV_FAKTURA_USR"),
            "SNOWFLAKE_FAKTURA_TRANSFORMER_PASSWORD": Variable.get(
                "SRV_FAKTURA_PASSWORD"
            ),
        },
        allowlist=[
            "hub.getdbt.com",
        ]
        + SNOW_ALLOWLIST,
        slack_channel=Variable.get("slack_error_channel"),
        retries=0,
    )


with DAG(
    "run_faktura__interday",
    start_date=datetime(2024, 10, 23),
    schedule_interval="0-59/10 4-19 * * *",
    max_active_runs=1,
    catchup=False,
) as dag:
    dbt_run = run_dbt_job("update_data")

    notify_slack_success = slack_success(dag=dag)

    # DAG
    dbt_run >> notify_slack_success
