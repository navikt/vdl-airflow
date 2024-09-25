import os

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.dates import days_ago
from kubernetes import client as k8s

from custom.operators.slack_operator import slack_success, test_slack

INBOUND_IMAGE = "europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/vdl-airflow-inbound@sha256:87fa26de608dedad7c31dd5af184d38fbd3345a3c8ef40ca0db41e2e382f1064"
DBT_IMAGE = "ghcr.io/dbt-labs/dbt-snowflake:1.8.3@sha256:b95cc0481ec39cb48f09d63ae0f912033b10b32f3a93893a385262f4ba043f50"
SNOW_ALLOWLIST = [
    "wx23413.europe-west4.gcp.snowflakecomputing.com",
    "ocsp.snowflakecomputing.com",
    "ocsp.digicert.com:80",
    "o.pki.goog:80",
    "ocsp.pki.goo:80",
    "storage.googleapis.com",
]
BRANCH = Variable.get("EIENDOM_BRANCH")


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
    dvh_eiendom__dim_org = last_fra_dvh_eiendom("dvh_eiendom__dim_org")
    dvh_eiendom__dim_geografi = last_fra_dvh_eiendom("dvh_eiendom__dim_geografi")
    dvh_eiendom__hrres_stillinger_eiendom = last_fra_dvh_eiendom(
        "dvh_eiendom__hrres_stillinger_eiendom"
    )
    dvh_eiendom__hrorg_orgstrukt_eiendom = last_fra_dvh_eiendom(
        "dvh_eiendom__hrorg_orgstrukt_eiendom"
    )
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
    dvh_eiendom__eiendom_statenslokaler_json = last_fra_dvh_eiendom("dvh_eiendom__eiendom_statenslokaler_json")
    dvh_eiendom__statlok_leieforhold = last_fra_dvh_eiendom("dvh_eiendom__statlok_leieforhold")
    dvh_eiendom__statlok_leieobjekt = last_fra_dvh_eiendom("dvh_eiendom__statlok_leieobjekt")
    dvh_eiendom__statlok_lokale = last_fra_dvh_eiendom("dvh_eiendom__statlok_lokale")

    mainmanager__dim_adresse = last_fra_mainmanager("mainmanager__dim_adresse")
    mainmanager__dim_bygg = last_fra_mainmanager("mainmanager__dim_bygg")
    mainmanager__dim_lokasjon = last_fra_mainmanager("mainmanager__dim_lokasjon")

    dvh_kodeverk__org_enhet_til_node = last_fra_dvh_eiendom(
        "dvh_kodeverk__org_enhet_til_node"
    )

    dbt_run = run_dbt_job("dbt_build")

    notify_slack_success = slack_success(dag=dag)

    # DAG
    dvh_eiendom__brukersted2lok >> dbt_run
    dvh_eiendom__eiendom_aarverk >> dbt_run
    dvh_eiendom__eiendom_aarverk_paa_lokasjon >> dbt_run
    dvh_eiendom__eiendom_aarverk_paa_lokasjon_dato >> dbt_run
    dvh_eiendom__eiendom_faktakorreksjon >> dbt_run
    dvh_eiendom__eiendom_matrikkel >> dbt_run
    dvh_eiendom__eiendom_matrikkelkorreksjon >> dbt_run
    dvh_eiendom__eiendom_matrikkel_veiadresse >> dbt_run
    dvh_eiendom__lyd_bygg >> dbt_run
    dvh_eiendom__lyd_lok_komp >> dbt_run
    dvh_eiendom__lyd_lok >> dbt_run
    dvh_eiendom__lyd_address >> dbt_run
    dvh_eiendom__lyd_postadr >> dbt_run
    dvh_eiendom__lyd_kommune >> dbt_run
    dvh_eiendom__lyd_county >> dbt_run
    dvh_eiendom__lyd_land >> dbt_run
    dvh_eiendom__dim_okonomi_aktivitet >> dbt_run
    dvh_eiendom__fak_eiendom_avtale >> dbt_run
    dvh_eiendom__dim_lokasjon >> dbt_run
    dvh_eiendom__lyd_loc_dt >> dbt_run
    dvh_eiendom__lyd_agreement >> dbt_run
    dvh_eiendom__dim_org >> dbt_run
    dvh_eiendom__dim_geografi >> dbt_run
    dvh_eiendom__hrres_stillinger_eiendom >> dbt_run
    dvh_eiendom__hrorg_orgstrukt_eiendom >> dbt_run
    dvh_eiendom__lyd_agreementitem >> dbt_run
    dvh_eiendom__lyd_amount >> dbt_run
    dvh_eiendom__lyd_avtaltyp >> dbt_run
    dvh_eiendom__lyd_dicipline >> dbt_run
    dvh_eiendom__lyd_doku_tab >> dbt_run
    dvh_eiendom__lyd_folder >> dbt_run
    dvh_eiendom__lyd_metatable >> dbt_run
    dvh_eiendom__lyd_orghierk >> dbt_run
    dvh_eiendom__lyd_price >> dbt_run
    dvh_eiendom__lyd_pricetype >> dbt_run
    dvh_eiendom__lyd_userdefinedfielddef >> dbt_run
    dvh_eiendom__lyd_userdefinedfields >> dbt_run
    dvh_eiendom__lyd_userdefinedfieldswide >> dbt_run
    dvh_eiendom__lyd_utl_stat >> dbt_run
    dvh_eiendom__eiendom_kor2024 >> dbt_run
    dvh_eiendom__eiendom_statenslokaler_json >> dbt_run
    dvh_eiendom__statlok_leieforhold >> dbt_run
    dvh_eiendom__statlok_leieobjekt >> dbt_run
    dvh_eiendom__statlok_lokale >> dbt_run

    mainmanager__dim_adresse >> dbt_run
    mainmanager__dim_bygg >> dbt_run
    mainmanager__dim_lokasjon >> dbt_run

    dvh_kodeverk__org_enhet_til_node >> dbt_run

    dbt_run >> notify_slack_success
