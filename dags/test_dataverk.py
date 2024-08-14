from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from dataverk_airflow import python_operator

with DAG("test_dataverk", start_date=days_ago(1), schedule_interval=None) as dag:
    t1 = python_operator(
        dag=dag,
        name="hello_world",
        repo="navikt/vdl-eiendom",
        branch="teste-dataverk-operators",
        script_path="test_airflow.py",
        image="europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/vdl-airflow-inbound@sha256:df16552fbba6affd60fc3c47ca3a1705f4ca5fa39bd396fe5393395a2dcd17e1",
        extra_envs={
            "EIENDOM_RAW_DB": Variable.get("EIENDOM_RAW_DB"),
            "MAINMANAGER_API_USERNAME": Variable.get("MAINMANAGER_API_USERNAME"),
            "MAINMANAGER_API_PASSWORD": Variable.get("MAINMANAGER_API_PASSWORD"),
            "MAINMANAGER_URL": Variable.get("MAINMANAGER_URL"),
            "SNOW_USR": Variable.get("SNOW_USR"),
            "SNOW_PWD": Variable.get("SNOW_PWD"),
        },
        allowlist=[
            "wx23413.europe-west4.gcp.snowflakecomputing.com",
            "ocsp.snowflakecomputing.com",
            "nav-test.mainmanager.no",
        ],
    )

    t1
