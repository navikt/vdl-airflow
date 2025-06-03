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

product_config = Variable.get(
        "config_run_budsjett_prognose", deserialize_json=True
    )


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
            "ANAPLAN_USR": product_config["anaplan_user"],
            "ANAPLAN_PWD": product_config["anaplan_password"],
            "SNOW_USR": Variable.get("srv_snowflake_user"),
            "SNOW_PWD": Variable.get("srv_snowflake_password"),
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
    "run_budsjett_prognose",
    start_date=days_ago(1),
    schedule_interval="@daily",  # Hver dag klokken 02:00 CEST
    max_active_runs=1,
) as dag:

    anaplan__budsjett = last_fra_anaplan("anaplan__ingest_budget")
    anaplan__prognose = last_fra_anaplan("anaplan__ingest_prognosis")

    notify_slack_success = slack_success(dag=dag)


    # DAG
    anaplan__budsjett >> notify_slack_success
    anaplan__prognose >> notify_slack_success