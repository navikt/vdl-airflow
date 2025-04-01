from airflow import DAG
from airflow.decorators import dag
from airflow.models import Variable
from airflow.utils.dates import days_ago

from custom.images import DBT_V_1_9
from custom.operators.slack_operator import slack_success

INBOUND_IMAGE = "europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/inbound@sha256:2ea798a469e615b74da8a243a8992a76a183527a5f5d9523f6911d553cbe44ff"
DBT_IMAGE = DBT_V_1_9
SNOW_ALLOWLIST = [
    "wx23413.europe-west4.gcp.snowflakecomputing.com",
    "ocsp.snowflakecomputing.com",
    "ocsp.digicert.com:80",
    "o.pki.goog:80",
    "ocsp.pki.goo:80",
    "storage.googleapis.com",
]
BRANCH = Variable.get("lonn_branch")


def last_fra_dvh_hr(inbound_job_name: str):
    from dataverk_airflow import python_operator

    return python_operator(
        dag=dag,
        name=inbound_job_name,
        repo="navikt/vdl-lonn",
        branch=BRANCH,
        script_path=f"ingest/run.py {inbound_job_name}",
        image=INBOUND_IMAGE,
        extra_envs={
            "LONN_RAW_DB": Variable.get("lonn_raw_db"),
            "LONN_LOADER_ROLE": Variable.get("lonn_loader_role"),
            "SNOW_USR": Variable.get("srv_snowflake_user"),
            "SNOW_PWD": Variable.get("srv_snowflake_password"),
            "DVH_USR": Variable.get("dvh_user"),
            "DVH_PWD": Variable.get("dvh_password"),
            "DVH_DSN": Variable.get("dvh_dsn"),
            "RUN_ID": "{{ run_id }}",
        },
        allowlist=[
            "dmv09-scan.adeo.no:1521",
        ]
        + SNOW_ALLOWLIST,
        slack_channel=Variable.get("slack_error_channel"),
    )

def run_dbt_job(job_name: str):
    from dataverk_airflow import kubernetes_operator

    return kubernetes_operator(
        dag=dag,
        name=job_name.replace(" ", "_"),
        repo="navikt/vdl-lonn",
        branch=BRANCH,
        working_dir="dbt",
        cmds=["dbt deps", f"{ job_name }"],
        image=DBT_IMAGE,
        extra_envs={
            "LONN_DB": Variable.get("lonn_db"),
            "SNOW_USR": Variable.get("srv_snowflake_user"),
            "SNOW_PWD": Variable.get("srv_snowflake_password"),
            "RUN_ID": "{{ run_id }}",
        },
        allowlist=[
            "hub.getdbt.com",
        ]
        + SNOW_ALLOWLIST,
        slack_channel=Variable.get("slack_error_channel"),
    )

with DAG(
    "run_lonn",
    start_date=days_ago(1),
    schedule_interval=None,
    max_active_runs=1,
) as dag:

    dvh_hr__hragg_aarsverk = last_fra_dvh_hr("dvh_hr__hragg_aarsverk")
    dvh_hr__hrorg_orgstrukt = last_fra_dvh_hr("dvh_hr__hrorg_orgstrukt")

    dbt_build = run_dbt_job("dbt build")

    notify_slack_success = slack_success(dag=dag)

    # DAG
    dvh_hr__hragg_aarsverk >> dbt_build
    dvh_hr__hrorg_orgstrukt >> dbt_build

    dbt_build >> notify_slack_success
