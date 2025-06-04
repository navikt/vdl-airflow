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

product_config = Variable.get("config_run_okonomimodell", deserialize_json=True)
snowflake_config = Variable.get("conn_snowflake", deserialize_json=True)


def run_dbt_job(job_name: str):
    from dataverk_airflow import kubernetes_operator

    return kubernetes_operator(
        dag=dag,
        name=job_name,
        repo="navikt/vdl-okonomimodell",
        branch=product_config["git_branch"],
        working_dir="dbt",
        cmds=[
            "dbt deps",
            "dbt snapshot",
            "dbt build",
        ],
        image=DBT_IMAGE,
        extra_envs={
            "OKONOMIMODELL_DB": product_config["dbt_db"],
            "DBT_USR": snowflake_config["user"],
            "DBT_PWD": snowflake_config["password"],
        },
        allowlist=[
            "hub.getdbt.com",
        ]
        + SNOW_ALLOWLIST,
        slack_channel=Variable.get("slack_error_channel"),
        retries=0,
    )


with DAG(
    "run_okonomimodell",
    start_date=datetime(2024, 11, 13),
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=False,
) as dag:
    dbt_run = run_dbt_job("update_data")

    notify_slack_success = slack_success(dag=dag)

    # DAG
    dbt_run >> notify_slack_success
