from datetime import datetime

from airflow import DAG
from airflow.decorators import dag
from airflow.models import Variable

from custom.images import DBT_V_1_9
from custom.operators.slack_operator import slack_success

DBT_IMAGE = DBT_V_1_9
SNOW_ALLOWLIST = [
    "wx23413.europe-west4.gcp.snowflakecomputing.com",
    "ocsp.snowflakecomputing.com",
    "ocsp.digicert.com:80",
    "o.pki.goog:80",
    "ocsp.pki.goo:80",
    "storage.googleapis.com",
]
BRANCH = Variable.get("REGNSKAP_BRANCH")


def run_dbt_job(job_name: str):
    from dataverk_airflow import kubernetes_operator

    return kubernetes_operator(
        dag=dag,
        name=job_name,
        repo="navikt/vdl-regnskapsdata",
        branch=BRANCH,
        working_dir="dbt",
        cmds=[
            "dbt deps",
            "dbt run -s \
                int_bilag__kontant__varm \
                int_bilag__regnskap__varm \
                int_hovedboksdetaljer__kontant__varm \
                int_hovedboksdetaljer__regnskap__varm \
                int_bilag_kunder_leverandor_forbindelser",
        ],
        image=DBT_IMAGE,
        extra_envs={
            "REGNSKAP_DB": Variable.get("REGNSKAP_DB"),
            "SRV_USR": Variable.get("SRV_REGNSKAP_USR"),
            "SRV_PWD": Variable.get("SRV_REGNSKAP_PWD"),
            "DBT_TARGET": "streamer",
        },
        allowlist=[
            "hub.getdbt.com",
        ]
        + SNOW_ALLOWLIST,
        slack_channel=Variable.get("slack_error_channel"),
        retries=0,
    )


with DAG(
    "run_regnskap__intercity_train",
    start_date=datetime(2024, 10, 23),
    schedule_interval="0-59/10 4-19 * * *",
    max_active_runs=1,
    catchup=False,
) as dag:
    dbt_run = run_dbt_job("update_data")
    notify_slack_success = slack_success(dag=dag)
    # DAG
    dbt_run >> notify_slack_success
