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
        slack_channel=Variable.get("slack_error_channel"),
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
        slack_channel=Variable.get("slack_error_channel"),
    )


with DAG(
    "run_eiendom", start_date=days_ago(1), schedule_interval=None, max_active_runs=1
) as dag:

    dvh_eiendom__brukersted2lok = last_fra_dvh_eiendom("dvh_eiendom__brukersted2lok")
    dvh_eiendom__eiendom_aarverk = last_fra_dvh_eiendom("dvh_eiendom__eiendom_aarverk")
    dvh_eiendom__eiendom_aarverk_paa_lokasjon = last_fra_dvh_eiendom(
        "dvh_eiendom__eiendom_aarverk_paa_lokasjon"
    )
    dvh_eiendom__eiendom_aarverk_paa_lokasjon_dato = last_fra_dvh_eiendom(
        "dvh_eiendom__eiendom_aarverk_paa_lokasjon_dato"
    )
    dvh_eiendom__eiendom_faktakorreksjon = last_fra_dvh_eiendom(
        "dvh_eiendom__eiendom_faktakorreksjon"
    )
    dvh_eiendom__eiendom_matrikkel = last_fra_dvh_eiendom(
        "dvh_eiendom__eiendom_matrikkel"
    )
    dvh_eiendom__eiendom_matrikkelkorreksjon = last_fra_dvh_eiendom(
        "dvh_eiendom__eiendom_matrikkelkorreksjon"
    )
    dvh_eiendom__eiendom_matrikkel_veiadresse = last_fra_dvh_eiendom(
        "dvh_eiendom__eiendom_matrikkel_veiadresse"
    )
    dvh_eiendom__lyd_bygg = last_fra_dvh_eiendom("dvh_eiendom__lyd_bygg")
    dvh_eiendom__lyd_lok_komp = last_fra_dvh_eiendom("dvh_eiendom__lyd_lok_komp")
    dvh_eiendom__lyd_lok = last_fra_dvh_eiendom("dvh_eiendom__lyd_lok")
    dvh_eiendom__lyd_address = last_fra_dvh_eiendom("dvh_eiendom__lyd_address")
    dvh_eiendom__lyd_postadr = last_fra_dvh_eiendom("dvh_eiendom__lyd_postadr")
    dvh_eiendom__lyd_kommune = last_fra_dvh_eiendom("dvh_eiendom__lyd_kommune")
    dvh_eiendom__lyd_county = last_fra_dvh_eiendom("dvh_eiendom__lyd_county")
    dvh_eiendom__lyd_land = last_fra_dvh_eiendom("dvh_eiendom__lyd_land")
    dvh_eiendom__dim_okonomi_aktivitet = last_fra_dvh_eiendom(
        "dvh_eiendom__dim_okonomi_aktivitet"
    )
    dvh_eiendom__fak_eiendom_avtale = last_fra_dvh_eiendom(
        "dvh_eiendom__fak_eiendom_avtale"
    )
    dvh_eiendom__dim_lokasjon = last_fra_dvh_eiendom("dvh_eiendom__dim_lokasjon")
    dvh_eiendom__lyd_loc_dt = last_fra_dvh_eiendom("dvh_eiendom__lyd_loc_dt")
    dvh_eiendom__lyd_agreement = last_fra_dvh_eiendom("dvh_eiendom__lyd_agreement")
    dvh_eiendom__org = last_fra_dvh_eiendom("dvh_eiendom__org")
    dvh_eiendom__hr_aarverk = last_fra_dvh_eiendom("dvh_eiendom__hr_aarverk")
    dvh_eiendom__lyd_agreementitem = last_fra_dvh_eiendom(
        "dvh_eiendom__lyd_agreementitem"
    )
    dvh_eiendom__lyd_amount = last_fra_dvh_eiendom("dvh_eiendom__lyd_amount")
    dvh_eiendom__lyd_avtaltyp = last_fra_dvh_eiendom("dvh_eiendom__lyd_avtaltyp")
    dvh_eiendom__lyd_dicipline = last_fra_dvh_eiendom("dvh_eiendom__lyd_dicipline")
    dvh_eiendom__lyd_doku_tab = last_fra_dvh_eiendom("dvh_eiendom__lyd_doku_tab")
    dvh_eiendom__lyd_folder = last_fra_dvh_eiendom("dvh_eiendom__lyd_folder")
    dvh_eiendom__lyd_metatable = last_fra_dvh_eiendom("dvh_eiendom__lyd_metatable")
    dvh_eiendom__lyd_orghierk = last_fra_dvh_eiendom("dvh_eiendom__lyd_orghierk")
    dvh_eiendom__lyd_price = last_fra_dvh_eiendom("dvh_eiendom__lyd_price")
    dvh_eiendom__lyd_pricetype = last_fra_dvh_eiendom("dvh_eiendom__lyd_pricetype")
    dvh_eiendom__lyd_userdefinedfielddef = last_fra_dvh_eiendom(
        "dvh_eiendom__lyd_userdefinedfielddef"
    )
    dvh_eiendom__lyd_userdefinedfields = last_fra_dvh_eiendom(
        "dvh_eiendom__lyd_userdefinedfields"
    )
    dvh_eiendom__lyd_userdefinedfieldswide = last_fra_dvh_eiendom(
        "dvh_eiendom__lyd_userdefinedfieldswide"
    )
    dvh_eiendom__lyd_utl_stat = last_fra_dvh_eiendom("dvh_eiendom__lyd_utl_stat")
    dvh_eiendom__eiendom_kor2024 = last_fra_dvh_eiendom("dvh_eiendom__eiendom_kor2024")

    mainmanager__dim_adresse = last_fra_mainmanager("mainmanager__dim_adresse")
    mainmanager__dim_bygg = last_fra_mainmanager("mainmanager__dim_bygg")
