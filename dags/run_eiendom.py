from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from dataverk_airflow import python_operator

INBOUND_IMAGE = "europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/vdl-airflow-inbound@sha256:5e62cb6d43653cb072dbeaff5b3632c0c8b0f62599b3fe170fc504bd881307aa"
SNOW_ALLOWLIST = [
    "wx23413.europe-west4.gcp.snowflakecomputing.com",
    "ocsp.snowflakecomputing.com",
    "ocsp.digicert.com:80",
    "o.pki.goog:80",
    "ocsp.pki.goo:80",
    "storage.googleapis.com",
]


def last_fra_mainmanager(inbound_job_name: str):
    return python_operator(
        dag=dag,
        name=inbound_job_name,
        repo="navikt/vdl-eiendom",
        branch="teste-dataverk-operators",
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
        ]
        + SNOW_ALLOWLIST,
    )


def last_fra_dvh_eiendom(inbound_job_name: str):
    return python_operator(
        dag=dag,
        name=inbound_job_name,
        repo="navikt/vdl-eiendom",
        branch="teste-dataverk-operators",
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
    )


with DAG("run_eiendom", start_date=days_ago(1), schedule_interval=None) as dag:
    mm_dim_adresse = last_fra_mainmanager("mainmanager__dim_adresse")

    mm_dim_bygg = last_fra_mainmanager("mainmanager__dim_bygg")

    dvh_eiendom__brukersted2lok = last_fra_dvh_eiendom("dvh_eiendom__brukersted2lok")

    mm_dim_adresse
    mm_dim_bygg
    dvh_eiendom__brukersted2lok