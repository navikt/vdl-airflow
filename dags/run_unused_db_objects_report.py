from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from dataverk_airflow import python_operator

from custom.operators.slack_operator import slack_success

with DAG(
    "run_unused_db_objects_report",
    start_date=datetime(year=2025, month=4, day=9),
    schedule_interval="0 9 * * 1",  # Hver mandag klokken 09:00 UTC
    max_active_runs=1,
) as dag:

    product_config = {
        "git_branch_okonomimodell": "main",
        "git_branch_eiendom": "unused-objects",
    }

    report_oko = python_operator(
        dag=dag,
        name="report_okonomimodell",
        repo="navikt/vdl-okonomimodell",
        branch=product_config["git_branch_okonomimodell"],
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

    report_to_slack_oko = slack_success(
        dag=dag,
        message="{{ task_instance.xcom_pull(task_ids='report_okonomimodell', key='return_value')['dump'] }}",
    )

    report_eiendom = python_operator(
        dag=dag,
        name="report_eiendom",
        repo="navikt/vdl-eiendom",
        branch=product_config["git_branch_eiendom"],
        script_path="orchestration/report_unused_db_objects.py",
        requirements_path="orchestration/requirements.txt",
        slack_channel=Variable.get("slack_error_channel"),
        do_xcom_push=True,
        extra_envs={
            "SNOWFLAKE_USER": Variable.get("srv_snowflake_user"),
            "SNOWFLAKE_PASSWORD": Variable.get("srv_snowflake_password"),
            "SNOWFLAKE_AUTHENTICATOR": "snowflake",
            "SNOWFLAKE_ROLE": "airflow_orchestrator",
            "SRV_USR": Variable.get("srv_snowflake_user"),
            "SRV_PWD": Variable.get("srv_snowflake_password"),
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

    report_to_slack_eiendom = slack_success(
        dag=dag,
        message="{{ task_instance.xcom_pull(task_ids='report_eiendom', key='return_value')['dump'] }}",
    )

    report_oko >> report_to_slack_oko
    report_eiendom >> report_to_slack_eiendom
