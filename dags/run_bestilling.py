import os

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from kubernetes import client as k8s

from custom.operators.slack_operator import slack_success, test_slack
from operators.elementary import elementary_operator

DBT_IMAGE = "ghcr.io/dbt-labs/dbt-snowflake:1.8.3@sha256:b95cc0481ec39cb48f09d63ae0f912033b10b32f3a93893a385262f4ba043f50"
ELEMENTARY_IMAGE = "europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/vdl-airflow-elementary@sha256:2f8187434db61ead6a32478ca82210733589c277dc8a4c744ccd0afe0c4f6610"
SNOW_ALLOWLIST = [
    "wx23413.europe-west4.gcp.snowflakecomputing.com",
    "ocsp.snowflakecomputing.com",
    "ocsp.digicert.com:80",
    "o.pki.goog:80",
    "ocsp.pki.goo:80",
    "storage.googleapis.com",
]
BRANCH = Variable.get("bestilling_branch")

def run_dbt_job(job_name: str):
    from dataverk_airflow import kubernetes_operator

    return kubernetes_operator(
        dag=dag,
        name=job_name.replace(" ", "_"),
        repo="navikt/vdl-innkjop",
        branch=BRANCH,
        working_dir="dbt",
        cmds=["dbt deps", f"{ job_name }"],
        image=DBT_IMAGE,
        extra_envs={
            "SRV_USR": Variable.get("srv_snowflake_user"),
            "SRV_PWD": Variable.get("srv_snowflake_password"),
            "RUN_ID": "{{ run_id }}",
            "DBT_TARGET": Variable.get("dbt_target"),
        },
        allowlist=[
            "hub.getdbt.com",
        ]
        + SNOW_ALLOWLIST,
        slack_channel=Variable.get("slack_error_channel"),
    )


def elementary(command: str):
    return elementary_operator(
        dag=dag,
        task_id=f"elementary_{command}",
        commands=[command],
        database=Variable.get("bestilling_db"),
        schema="meta",
        snowflake_role="innkjop_transformer",
        snowflake_warehouse="innkjop_transformer",
        dbt_docs_project_name="innkjop",
        image=ELEMENTARY_IMAGE,
    )


with DAG(
    "run_innkjop",
    start_date=days_ago(1),
    schedule_interval="0 5 * * *",  # Hver dag klokken 05:00 UTC
    max_active_runs=1,
) as dag:

    dbt_build = run_dbt_job("dbt build")

    notify_slack_success = slack_success(dag=dag, channel="#virksomhetsdatalaget-info-test")

    elementary__report = elementary("dbt_docs")

    # DAG
   
    dbt_build >> elementary__report
    dbt_build >> notify_slack_success
