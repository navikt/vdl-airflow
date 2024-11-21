import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from kubernetes import client as k8s

from custom.operators.slack_operator import slack_success, test_slack
from operators.elementary import elementary_operator

INBOUND_IMAGE = "europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/inbound@sha256:f97c7e4df670e1ec345ff2235e32befbedb944afb9dfeefe57be902bc13e47b4"
DBT_IMAGE = "ghcr.io/dbt-labs/dbt-snowflake:1.8.3@sha256:b95cc0481ec39cb48f09d63ae0f912033b10b32f3a93893a385262f4ba043f50"
ELEMENTARY_IMAGE = "europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/vdl-airflow-elementary@sha256:28933e3dc935645c9d22d2e0d0794e84e9f6725b5627f007332cc2b23d93d978"
SNOW_ALLOWLIST = [
    "wx23413.europe-west4.gcp.snowflakecomputing.com",
    "ocsp.snowflakecomputing.com",
    "ocsp.digicert.com:80",
    "o.pki.goog:80",
    "ocsp.pki.goo:80",
    "storage.googleapis.com",
]
BRANCH = Variable.get("eiendom_branch")


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


with DAG(
    "dvh_eiendom",
    start_date=datetime(2024, 11, 20),
    schedule_interval=None,
    max_active_runs=1,
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
    dvh_eiendom__eiendom_statenslokaler_json = last_fra_dvh_eiendom(
        "dvh_eiendom__eiendom_statenslokaler_json"
    )
    dvh_eiendom__statlok_leieforhold = last_fra_dvh_eiendom(
        "dvh_eiendom__statlok_leieforhold"
    )
    dvh_eiendom__statlok_leieobjekt = last_fra_dvh_eiendom(
        "dvh_eiendom__statlok_leieobjekt"
    )
    dvh_eiendom__statlok_lokale = last_fra_dvh_eiendom("dvh_eiendom__statlok_lokale")

    # DAG
    dvh_eiendom__brukersted2lok
    dvh_eiendom__eiendom_aarverk
    dvh_eiendom__eiendom_aarverk_paa_lokasjon
    dvh_eiendom__eiendom_aarverk_paa_lokasjon_dato
    dvh_eiendom__eiendom_faktakorreksjon
    dvh_eiendom__eiendom_matrikkel
    dvh_eiendom__eiendom_matrikkelkorreksjon
    dvh_eiendom__eiendom_matrikkel_veiadresse
    dvh_eiendom__lyd_bygg
    dvh_eiendom__lyd_lok_komp
    dvh_eiendom__lyd_lok
    dvh_eiendom__lyd_address
    dvh_eiendom__lyd_postadr
    dvh_eiendom__lyd_kommune
    dvh_eiendom__lyd_county
    dvh_eiendom__lyd_land
    dvh_eiendom__dim_okonomi_aktivitet
    dvh_eiendom__fak_eiendom_avtale
    dvh_eiendom__dim_lokasjon
    dvh_eiendom__lyd_loc_dt
    dvh_eiendom__lyd_agreement
    dvh_eiendom__hrres_stillinger_eiendom
    dvh_eiendom__hrorg_orgstrukt_eiendom
    dvh_eiendom__lyd_agreementitem
    dvh_eiendom__lyd_amount
    dvh_eiendom__lyd_avtaltyp
    dvh_eiendom__lyd_dicipline
    dvh_eiendom__lyd_doku_tab
    dvh_eiendom__lyd_folder
    dvh_eiendom__lyd_metatable
    dvh_eiendom__lyd_orghierk
    dvh_eiendom__lyd_price
    dvh_eiendom__lyd_pricetype
    dvh_eiendom__lyd_userdefinedfielddef
    dvh_eiendom__lyd_userdefinedfields
    dvh_eiendom__lyd_userdefinedfieldswide
    dvh_eiendom__lyd_utl_stat
    dvh_eiendom__eiendom_kor2024
    dvh_eiendom__eiendom_statenslokaler_json
    dvh_eiendom__statlok_leieforhold
    dvh_eiendom__statlok_leieobjekt
    dvh_eiendom__statlok_lokale
