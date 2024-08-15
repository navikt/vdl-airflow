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
        image="europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/vdl-airflow-inbound@sha256:53a6b87eef22bfa7518dc3cabb8ac1389836a201de5f1518ecea7da51add221a",
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
