import os

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from kubernetes import client as k8s

from custom.operators.slack_operator import slack_success, test_slack
from operators.elementary import elementary_operator

INBOUND_IMAGE = "europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/inbound@sha256:f97c7e4df670e1ec345ff2235e32befbedb944afb9dfeefe57be902bc13e47b4"
DBT_IMAGE = "ghcr.io/dbt-labs/dbt-snowflake:1.8.3@sha256:b95cc0481ec39cb48f09d63ae0f912033b10b32f3a93893a385262f4ba043f50"
ELEMENTARY_IMAGE = "europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/vdl-airflow-elementary@sha256:eae098d3183dd13094dccd29cbc3f16fcca9023f83ba51f693c5cd56ee234076"
SNOW_ALLOWLIST = [
    "wx23413.europe-west4.gcp.snowflakecomputing.com",
    "ocsp.snowflakecomputing.com",
    "ocsp.digicert.com:80",
    "o.pki.goog:80",
    "ocsp.pki.goo:80",
    "storage.googleapis.com",
]
BRANCH = Variable.get("eiendom_branch")


def last_fra_mainmanager(inbound_job_name: str):
    from dataverk_airflow import python_operator

    return python_operator(
        dag=dag,
        name=inbound_job_name,
        repo="navikt/vdl-eiendom",
        branch=BRANCH,
        script_path=f"ingest/run.py {inbound_job_name}",
        image=INBOUND_IMAGE,
        extra_envs={
            "EIENDOM_RAW_DB": Variable.get("EIENDOM_RAW_DB"),
            "MAINMANAGER_API_USERNAME": Variable.get("MAINMANAGER_API_USERNAME"),
            "MAINMANAGER_API_PASSWORD": Variable.get("MAINMANAGER_API_PASSWORD"),
            "MAINMANAGER_URL": Variable.get("MAINMANAGER_URL"),
            "SNOW_USR": Variable.get("SNOW_USR"),
            "SNOW_PWD": Variable.get("SNOW_PWD"),
            "RUN_ID": "{{ run_id }}",
        },
        allowlist=[
            "nav-test.mainmanager.no",
            "nav.mainmanager.no",
        ]
        + SNOW_ALLOWLIST,
        slack_channel=Variable.get("slack_error_channel"),
    )


def last_fra_dvh_eiendom(inbound_job_name: str):
    from dataverk_airflow import python_operator

    return python_operator(
        dag=dag,
        name=inbound_job_name,
        repo="navikt/vdl-eiendom",
        branch=BRANCH,
        script_path=f"ingest/run.py {inbound_job_name}",
        image=INBOUND_IMAGE,
        extra_envs={
            "EIENDOM_RAW_DB": Variable.get("EIENDOM_RAW_DB"),
            "SNOW_USR": Variable.get("SNOW_USR"),
            "SNOW_PWD": Variable.get("SNOW_PWD"),
            "DVH_USR": Variable.get("DVH_USR"),
            "DVH_PWD": Variable.get("DVH_PWD"),
            "DVH_DSN": Variable.get("DVH_DSN"),
            "RUN_ID": "{{ run_id }}",
        },
        allowlist=[
            "dm08-scan.adeo.no:1521",
        ]
        + SNOW_ALLOWLIST,
        slack_channel=Variable.get("slack_error_channel"),
    )


def run_dbt_job(job_name: str):
    from dataverk_airflow import kubernetes_operator

    return kubernetes_operator(
        dag=dag,
        name=job_name,
        repo="navikt/vdl-eiendom",
        branch=BRANCH,
        working_dir="dbt",
        cmds=["dbt deps", "dbt build"],
        image=DBT_IMAGE,
        extra_envs={
            "EIENDOM_DB": Variable.get("EIENDOM_DB"),
            "SRV_USR": Variable.get("SRV_USR"),
            "SRV_PWD": Variable.get("SRV_PWD"),
            "SNOW_USR": Variable.get("SNOW_USR"),
            "SNOW_PWD": Variable.get("SNOW_PWD"),
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
        allowlist=["slack.com", "files.slack.com", Variable.get("dbt_docs_url")]
        + SNOW_ALLOWLIST,
        extra_envs={
            "DB": Variable.get("eiendom_db"),
            "DB_ROLE": "eiendom_transformer",
            "DB_WH": "eiendom_transformer",
            "DBT_PROSJEKT": "eiendom",
        },
        image=ELEMENTARY_IMAGE,
    )


with DAG(
    "run_eiendom",
    start_date=days_ago(1),
    schedule_interval="0 4 * * *",  # Hver dag klokken 04:00 UTC
    max_active_runs=1,
) as dag:

    mainmanager__grouping = EmptyOperator(task_id="mainmanager__grouping")
    mainmanager__dim_adresse = last_fra_mainmanager("mainmanager__dim_adresse")
    mainmanager__dim_bygg = last_fra_mainmanager("mainmanager__dim_bygg")
    mainmanager__dim_eiendom = last_fra_mainmanager("mainmanager__dim_eiendom")
    mainmanager__dim_eiendomstype = last_fra_mainmanager(
        "mainmanager__dim_eiendomstype"
    )
    mainmanager__dim_grunneiendom = last_fra_mainmanager(
        "mainmanager__dim_grunneiendom"
    )
    mainmanager__oversettelser = last_fra_mainmanager("mainmanager__oversettelser")
    mainmanager__artikler = last_fra_mainmanager("mainmanager__artikler")
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

    dvh_kodeverk__org_enhet_til_node = last_fra_dvh_eiendom(
        "dvh_kodeverk__org_enhet_til_node"
    )
    dvh_kodeverk__dim_org = last_fra_dvh_eiendom("dvh_kodeverk__dim_org")
    dvh_kodeverk__dim_geografi = last_fra_dvh_eiendom("dvh_kodeverk__dim_geografi")
    dvh_kodeverk__dim_virksomhet = last_fra_dvh_eiendom("dvh_kodeverk__dim_virksomhet")
    dvh_hr__hragg_aarsverk = last_fra_dvh_eiendom("dvh_hr__hragg_aarsverk")

    dbt_run = run_dbt_job("dbt_build")

    notify_slack_success = slack_success(dag=dag)

    elementary__report = elementary("dbt_docs")

    # DAG
    mainmanager__grouping >> mainmanager__dim_adresse >> dbt_run
    mainmanager__grouping >> mainmanager__dim_bygg >> dbt_run
    mainmanager__grouping >> mainmanager__dim_eiendom >> dbt_run
    mainmanager__grouping >> mainmanager__dim_eiendomstype >> dbt_run
    mainmanager__grouping >> mainmanager__dim_grunneiendom >> dbt_run
    mainmanager__grouping >> mainmanager__oversettelser >> dbt_run
    mainmanager__grouping >> mainmanager__artikler >> dbt_run
    mainmanager__grouping >> mainmanager__fak_hovedleiekontrakt >> dbt_run
    mainmanager__grouping >> mainmanager__dim_framleie1 >> dbt_run
    mainmanager__grouping >> mainmanager__dim_framleie2 >> dbt_run
    mainmanager__grouping >> mainmanager__fak_arealtall >> dbt_run
    mainmanager__grouping >> mainmanager__fak_avtalepost_hoved >> dbt_run
    mainmanager__grouping >> mainmanager__fak_avtalepost_fremleie1 >> dbt_run
    mainmanager__grouping >> mainmanager__fak_avtalepost_fremleie2 >> dbt_run

    dvh_kodeverk__org_enhet_til_node >> dbt_run
    dvh_kodeverk__dim_org >> dbt_run
    dvh_kodeverk__dim_geografi >> dbt_run
    dvh_kodeverk__dim_virksomhet >> dbt_run

    dvh_hr__hragg_aarsverk >> dbt_run

    dbt_run >> notify_slack_success
    dbt_run >> elementary__report
