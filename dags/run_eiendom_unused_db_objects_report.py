from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from dataverk_airflow import python_operator

from custom.operators.slack_operator import slack_success

with DAG(
    "run_eiendom_unused_db_objects_report",
    start_date=datetime(year=2025, month=4, day=9),
    schedule_interval="0 9 * * 1",  # Hver mandag klokken 09:00 UTC
    max_active_runs=1,
) as dag:
    report = python_operator(
        dag=dag,
        name="unused_db_objects_report",
        repo="navikt/vdl-eiendom",
        branch="unused-objects",
        script_path="orchestration/report_unused_db_objects.py",
        requirements_path="orchestration/requirements.txt",
        slack_channel=Variable.get("slack_error_channel"),
        do_xcom_push=True,
        extra_envs={
            "SNOWFLAKE_USER": Variable.get("srv_snowflake_user"),
            "SNOWFLAKE_PASSWORD": Variable.get("srv_snowflake_password"),
            "SNOWFLAKE_AUTHENTICATOR": "snowflake",
            "SNOWFLAKE_ROLE": "airflow_orchestrator",
        },
        allowlist=[
            "wx23413.europe-west4.gcp.snowflakecomputing.com",
            "ocsp.snowflakecomputing.com",
            "ocsp.digicert.com:80",
            "o.pki.goog:80",
            "ocsp.pki.goo:80",
            "storage.googleapis.com",
            "hub.getdbt.com",
        ],
    )

    send_report_to_slack = slack_success(
        dag=dag,
        message="{{ task_instance.xcom_pull(task_ids='unused_db_objects_report', key='return_value')['dump'] }}",
    )

    report >> send_report_to_slack
