from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from dataverk_airflow import python_operator

from custom.operators.slack_operator import slack_success

with DAG(
    "run_potential_drop_object_report",
    start_date=datetime(year=2025, month=4, day=9),
    schedule_interval="0 9 * * 1",  # Hver mandag klokken 09:00 UTC
    max_active_runs=1,
) as dag:
    report = python_operator(
        dag=dag,
        name="drop_table_report",
        repo="navikt/vdl-airflow",
        branch="drop-tables",
        script_path="waste_report/potential_object_removal.py",
        requirements_path="waste_report/requirements.txt",
        slack_channel=Variable.get("slack_error_channel"),
        do_xcom_push=True,
        extra_envs={
            "SNOWFLAKE_USER": Variable.get("srv_snowflake_user"),
            "SNOWFLAKE_PASSWORD": Variable.get("srv_snowflake_password"),
            "SNOWFLAKE_AUTHENTICATOR": "snowflake",
        },
        allowlist=[
            "wx23413.europe-west4.gcp.snowflakecomputing.com",
            "ocsp.snowflakecomputing.com",
            "ocsp.digicert.com:80",
            "o.pki.goog:80",
            "ocsp.pki.goo:80",
            "storage.googleapis.com",
        ],
    )

    # notify_slack_success = slack_success(dag=dag)
