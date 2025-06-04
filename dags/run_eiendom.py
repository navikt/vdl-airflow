import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from kubernetes import client as k8s

from custom.operators.slack_operator import slack_success, test_slack
from operators.elementary import elementary_operator

INBOUND_IMAGE = "europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/inbound@sha256:2ea798a469e615b74da8a243a8992a76a183527a5f5d9523f6911d553cbe44ff"
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
product_config = Variable.get("config_run_eiendom", deserialize_json=True)
snowflake_config = Variable.get("conn_snowflake", deserialize_json=True)
mainmanager_config = Variable.get("conn_mainmanager", deserialize_json=True)
dvh_config = Variable.get("conn_dvh", deserialize_json=True)


def last_fra_mainmanager(inbound_job_name: str):
    from dataverk_airflow import python_operator

    return python_operator(
        dag=dag,
        name=inbound_job_name,
        repo="navikt/vdl-eiendom",
        branch=product_config["git_branch"],
        script_path=f"ingest/run.py {inbound_job_name}",
        image=INBOUND_IMAGE,
        extra_envs={
            "EIENDOM_RAW_DB": product_config["raw_db"],
            "MAINMANAGER_API_USERNAME": mainmanager_config["username"],
            "MAINMANAGER_API_PASSWORD": mainmanager_config["password"],
            "MAINMANAGER_URL": mainmanager_config["url"],
            "SNOW_USR": snowflake_config["user"],
            "SNOW_PWD": snowflake_config["password"],
            "SNOW_ROLE": product_config["snow_role"],
            "RUN_ID": "{{ run_id }}",
        },
        allowlist=["nav.mainmanager.no"] + SNOW_ALLOWLIST,
        slack_channel=Variable.get("slack_error_channel"),
    )


def last_fra_dvh_eiendom(inbound_job_name: str):
    from dataverk_airflow import python_operator

    return python_operator(
        dag=dag,
        name=inbound_job_name,
        repo="navikt/vdl-eiendom",
        branch=product_config["git_branch"],
        script_path=f"ingest/run.py {inbound_job_name}",
        image=INBOUND_IMAGE,
        extra_envs={
            "EIENDOM_RAW_DB": product_config["raw_db"],
            "SNOW_USR": snowflake_config["user"],
            "SNOW_PWD": snowflake_config["password"],
            "SNOW_ROLE": product_config["snow_role"],
            "DVH_USR": dvh_config["user"],
            "DVH_PWD": dvh_config["password"],
            "DVH_DSN": dvh_config["dsn"],
            "RUN_ID": "{{ run_id }}",
        },
        allowlist=["dmv09-scan.adeo.no:1521"] + SNOW_ALLOWLIST,
        slack_channel=Variable.get("slack_error_channel"),
    )


def run_dbt_job(job_name: str):
    from dataverk_airflow import kubernetes_operator

    return kubernetes_operator(
        dag=dag,
        name=job_name.replace(" ", "_"),
        repo="navikt/vdl-eiendom",
        branch=product_config["git_branch"],
        working_dir="dbt",
        cmds=["dbt deps", f"{ job_name }"],
        image=DBT_IMAGE,
        extra_envs={
            "SRV_USR": snowflake_config["user"],
            "SRV_PWD": snowflake_config["password"],
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
        database=product_config["dbt_db"],
        schema="meta",
        snowflake_role="eiendom_transformer",
        snowflake_warehouse="eiendom_transformer",
        dbt_docs_project_name="eiendom",
        image=ELEMENTARY_IMAGE,
    )


with DAG(
    "run_eiendom",
    start_date=datetime(2024, 8, 21),
    schedule_interval="0 4 * * *",  # Hver dag klokken 04:00 UTC
    max_active_runs=1,
    catchup=False,
) as dag:

    mainmanager__grouping = EmptyOperator(task_id="mainmanager__grouping")
    mainmanager__dim_adresse = last_fra_mainmanager("mainmanager__dim_adresse")
    mainmanager__dim_bygg = last_fra_mainmanager("mainmanager__dim_bygg")
    mainmanager__dim_eiendom = last_fra_mainmanager("mainmanager__dim_eiendom")
    mainmanager__dim_eiendomstype = last_fra_mainmanager(
        "mainmanager__dim_eiendomstype"
    )
    mainmanager__dim_eiendomskategori = last_fra_mainmanager(
        "mainmanager__dim_eiendomskategori"
    )
    mainmanager__dim_grunneiendom = last_fra_mainmanager(
        "mainmanager__dim_grunneiendom"
    )
    mainmanager__oversettelser = last_fra_mainmanager("mainmanager__oversettelser")
    mainmanager__artikler = last_fra_mainmanager("mainmanager__artikler")
    mainmanager__organisasjon = last_fra_mainmanager("mainmanager__organisasjon")
    mainmanager__fak_hovedleiekontrakt = last_fra_mainmanager(
        "mainmanager__fak_hovedleiekontrakt"
    )
    mainmanager__dim_framleie1 = last_fra_mainmanager("mainmanager__dim_framleie1")
    mainmanager__dim_framleie2 = last_fra_mainmanager("mainmanager__dim_framleie2")
    mainmanager__fak_arealtall = last_fra_mainmanager("mainmanager__fak_arealtall")
    mainmanager__fak_avtalepost_hoved = last_fra_mainmanager(
        "mainmanager__fak_avtalepost_hoved"
    )
    mainmanager__fak_avtalepost_fremleie1 = last_fra_mainmanager(
        "mainmanager__fak_avtalepost_fremleie1"
    )
    mainmanager__fak_avtalepost_fremleie2 = last_fra_mainmanager(
        "mainmanager__fak_avtalepost_fremleie2"
    )

    dvh_kodeverk__grouping = EmptyOperator(task_id="dvh_kodeverk__grouping")
    dvh_kodeverk__dim_virksomhet = last_fra_dvh_eiendom("dvh_kodeverk__dim_virksomhet")

    dvh_eiendom__grouping = EmptyOperator(task_id="dvh_eiendom__grouping")
    dvh_eiendom__hrres_stillinger_eiendom = last_fra_dvh_eiendom(
        "dvh_eiendom__hrres_stillinger_eiendom"
    )

    dbt_build = run_dbt_job("dbt build")

    notify_slack_success = slack_success(dag=dag)

    elementary__report = elementary("dbt_docs")

    # DAG
    mainmanager__dim_adresse >> mainmanager__grouping
    mainmanager__dim_bygg >> mainmanager__grouping
    mainmanager__dim_eiendom >> mainmanager__grouping
    mainmanager__dim_eiendomstype >> mainmanager__grouping
    mainmanager__dim_eiendomskategori >> mainmanager__grouping
    mainmanager__dim_grunneiendom >> mainmanager__grouping
    mainmanager__oversettelser >> mainmanager__grouping
    mainmanager__artikler >> mainmanager__grouping
    mainmanager__organisasjon >> mainmanager__grouping
    mainmanager__fak_hovedleiekontrakt >> mainmanager__grouping
    mainmanager__dim_framleie1 >> mainmanager__grouping
    mainmanager__dim_framleie2 >> mainmanager__grouping
    mainmanager__fak_arealtall >> mainmanager__grouping
    mainmanager__fak_avtalepost_hoved >> mainmanager__grouping
    mainmanager__fak_avtalepost_fremleie1 >> mainmanager__grouping
    mainmanager__fak_avtalepost_fremleie2 >> mainmanager__grouping

    dvh_kodeverk__dim_virksomhet >> dvh_kodeverk__grouping

    dvh_eiendom__hrres_stillinger_eiendom >> dvh_eiendom__grouping

    mainmanager__grouping >> dbt_build
    dvh_kodeverk__grouping >> dbt_build
    dvh_eiendom__grouping >> dbt_build

    dbt_build >> elementary__report
    dbt_build >> notify_slack_success
