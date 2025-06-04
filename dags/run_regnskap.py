from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.models import Variable
from airflow.utils.dates import days_ago

from custom.images import DBT_V_1_9
from custom.operators.slack_operator import slack_success
from operators.elementary import elementary_operator

DBT_IMAGE = DBT_V_1_9
INBOUND_IMAGE = "europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/inbound@sha256:7bbe21e651aabec7e50fad053ebc315b6318c659988c2f71beb554e3994c381a"
ELEMENTARY_IMAGE = "europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/vdl-airflow-elementary@sha256:2f8187434db61ead6a32478ca82210733589c277dc8a4c744ccd0afe0c4f6610"
SNOW_ALLOWLIST = [
    "wx23413.europe-west4.gcp.snowflakecomputing.com",
    "ocsp.snowflakecomputing.com",
    "ocsp.digicert.com:80",
    "o.pki.goog:80",
    "ocsp.pki.goo:80",
    "storage.googleapis.com",
]

product_config = Variable.get("config_run_regnskap", deserialize_json=True)
snowflake_config = Variable.get("conn_snowflake", deserialize_json=True)
anaplan_config = Variable.get("conn_anaplan", deserialize_json=True)


def last_fra_anaplan(inbound_job_name: str):
    from dataverk_airflow import python_operator

    return python_operator(
        dag=dag,
        name=inbound_job_name,
        repo="navikt/vdl-regnskapsdata",
        branch=product_config["git_branch"],
        script_path=f"ingest/run.py {inbound_job_name}",
        image=INBOUND_IMAGE,
        extra_envs={
            "REGNSKAP_RAW_DB": product_config["raw_db"],
            "ANAPLAN_USR": anaplan_config["user"],
            "ANAPLAN_PWD": anaplan_config["password"],
            "SNOW_USR": snowflake_config["user"],
            "SNOW_PWD": snowflake_config["password"],
            "RUN_ID": "{{ run_id }}",
        },
        allowlist=[
            "api.anaplan.com",
            "auth.anaplan.com",
        ]
        + SNOW_ALLOWLIST,
        slack_channel=Variable.get("slack_error_channel"),
    )


def run_dbt_job(job_name: str):
    from dataverk_airflow import kubernetes_operator

    return kubernetes_operator(
        dag=dag,
        name=job_name.replace(" ", "_"),
        repo="navikt/vdl-regnskapsdata",
        branch=product_config["git_branch"],
        working_dir="dbt",
        cmds=["dbt deps", f"{ job_name }"],
        image=DBT_IMAGE,
        extra_envs={
            "SRV_USR": snowflake_config["user"],
            "SRV_PWD": snowflake_config["password"],
            "DBT_TARGET": product_config["dbt_target"],
            "REGNSKAP_DB": product_config["dbt_db"],
            "RUN_ID": "{{ run_id }}",
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
        database=product_config["dbt_db"],
        schema="meta",
        snowflake_role=product_config["elementary_role"],
        snowflake_warehouse=product_config["elementary_warehouse"],
        dbt_docs_project_name=product_config["dbt_docs_project_name"],
        image=ELEMENTARY_IMAGE,
    )


with DAG(
    "run_regnskap",
    start_date=days_ago(1),
    schedule_interval="@daily",  # Hver dag klokken 00:00 UTC (02:00 CEST)
    outlets=[Dataset("regnskap_dataset")],
    catchup=False,
    max_active_runs=1,
) as dag:

    anaplan__budsjett = last_fra_anaplan("anaplan__ingest_budget")
    anaplan__prognose = last_fra_anaplan("anaplan__ingest_prognosis")

    dbt_build = run_dbt_job("dbt build")

    notify_slack_success = slack_success(dag=dag)

    elementary__report = elementary("dbt_docs")

    # DAG
    anaplan__budsjett >> dbt_build
    anaplan__prognose >> dbt_build
    dbt_build >> elementary__report
    dbt_build >> notify_slack_success
