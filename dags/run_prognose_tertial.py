from airflow import DAG
from airflow.decorators import dag
from airflow.models import Variable
from airflow.utils.dates import days_ago

from custom.operators.slack_operator import slack_success

INBOUND_IMAGE = "europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/inbound@sha256:7bbe21e651aabec7e50fad053ebc315b6318c659988c2f71beb554e3994c381a"

SNOW_ALLOWLIST = [
    "wx23413.europe-west4.gcp.snowflakecomputing.com",
    "ocsp.snowflakecomputing.com",
    "ocsp.digicert.com:80",
    "o.pki.goog:80",
    "ocsp.pki.goo:80",
    "storage.googleapis.com",
]

product_config = Variable.get("config_run_budsjett_prognose", deserialize_json=True)
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


with DAG(
    "run_prognose_tertial",
    start_date=days_ago(1),
    schedule_interval=None,  # prognose tertial kjøres kun manuelt, ved behov
    catchup=False,
    max_active_runs=1,
) as dag:

    anaplan__prognose_tertial = last_fra_anaplan("anaplan__ingest_prognosis_tertial")

    notify_slack_success = slack_success(dag=dag)

    # DAG
    anaplan__prognose_tertial >> notify_slack_success
