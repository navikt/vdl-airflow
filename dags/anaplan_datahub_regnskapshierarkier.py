from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable
from kubernetes import client as k8s

from custom.decorators import CUSTOM_IMAGE
from custom.operators.slack_operator import slack_error, slack_info, slack_success_old


@dag(
    start_date=datetime(2023, 8, 2),
    schedule_interval=None,
    schedule=[Dataset("regnskap_dataset")],
    catchup=False,
    default_args={"on_failure_callback": slack_error, "retries": 3},
)
def anaplan_datahub_regnskaphierarkier():
    config_anaplan = Variable.get("conn_anaplan", deserialize_json=True)
    wGuid = config_anaplan.get("workspace_id")
    mGuid = config_anaplan.get("model_id")
    username = config_anaplan.get("user")
    password = config_anaplan.get("password")

    @task(
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    annotations={
                        "allowlist": ",".join(
                            [
                                "slack.com",
                            ]
                        )
                    }
                ),
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            image=CUSTOM_IMAGE,
                        )
                    ]
                ),
            )
        },
    )
    def send_slack_notification():
        slack_success_old()

    @task(
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    annotations={
                        "allowlist": ",".join(
                            [
                                "slack.com",
                                "api.anaplan.com",
                                "auth.anaplan.com",
                                "wx23413.europe-west4.gcp.snowflakecomputing.com",
                            ]
                        )
                    }
                ),
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            image=CUSTOM_IMAGE,
                        )
                    ]
                ),
            )
        },
    )
    def transfer(
        fileData: dict,
        query: str,
        import_hierarchy_data: dict,
        import_module_data: dict,
    ):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        from anaplan.get_data import get_data, transform_to_csv
        from anaplan.import_data import import_data
        from anaplan.singleChunkUpload import transfer_data

        with SnowflakeHook().get_cursor() as cursor:
            data = get_data(query, cursor)

        csv_file = transform_to_csv(data[0], data[1])

        transfer_data(wGuid, mGuid, username, password, fileData, csv_file)
        import_data(wGuid, mGuid, username, password, import_hierarchy_data)
        import_data(wGuid, mGuid, username, password, import_module_data)

    upload_artskonti = transfer.override(task_id="transfer_artskonti")(
        fileData={"id": "113000000029", "name": "dim_artskonti_snowflake.csv"},
        query="""
            with
                src as (
                    select
                        pk_dim_artskonti,
                        kode,
                        beskrivelse,
                        ar,
                        posterbar_fra_dato,
                        posterbar_til_dato,
                        er_summeringsniva,
                        er_posterbar,
                        er_budsjetterbar,
                        er_aktiv,
                        er_siste_gyldige,
                        har_hierarki,
                        artskonto,
                        artskonto_beskrivelse,
                        konto_tre_siffer,
                        konto_tre_siffer_beskrivelse,
                        budsjett_niva,
                        budsjett_niva_beskrivelse,
                        kontogruppe,
                        kontogruppe_beskrivelse,
                        kontoklasse,
                        kontoklasse_beskrivelse,
                        artskonto_totalniva,
                        artskonto_totalniva_beskrivelse
                    from okonomimodell.marts.dim_artskonti
                ),
                depricated as (
                    select
                        *,
                        kode as artskonti_segment_kode,
                        artskonto as artskonti_segment_kode_niva_4,
                        konto_tre_siffer as artskonti_segment_kode_niva_3,
                        budsjett_niva as artskonti_segment_kode_niva_2_5,
                        kontogruppe as artskonti_segment_kode_niva_2,
                        kontoklasse as artskonti_segment_kode_niva_1,
                        artskonto_totalniva as artskonti_segment_kode_niva_0,
                        beskrivelse as artskonti_segment_beskrivelse,
                        artskonto_beskrivelse as artskonti_segment_beskrivelse_niva_4,
                        konto_tre_siffer_beskrivelse as artskonti_segment_beskrivelse_niva_3,
                        budsjett_niva_beskrivelse as artskonti_segment_beskrivelse_niva_2_5,
                        kontogruppe_beskrivelse as artskonti_segment_beskrivelse_niva_2,
                        kontoklasse_beskrivelse as artskonti_segment_beskrivelse_niva_1,
                        artskonto_totalniva_beskrivelse as artskonti_segment_beskrivelse_niva_0,
                        0 as er_ytelse_konto,
                        har_hierarki as _har_hierarki
                    from src
                ),
                final as (select * from depricated)

            select *
            from final
        """,
        import_hierarchy_data={
            "id": "112000000041",
            "name": "Artskonti Flat from dim_artskonti_snowflake.csv",
        },
        import_module_data={
            "id": "112000000066",
            "name": "SYS 02.01 Kontostruktur from dim_artskonti_snowflake.csv",
        },
    )

    upload_kostnadssteder = transfer.override(task_id="transfer_kostnadssteder")(
        fileData={"id": "113000000030", "name": "dim_kostnadssteder_snowflake.csv"},
        query="""
            with
                src as (
                    select
                        pk_dim_kostnadssteder,
                        kostnadssted,
                        kostnadssted_beskrivelse,
                        ar,
                        posterbar_fra_dato,
                        posterbar_til_dato,
                        er_summeringsniva,
                        er_posterbar,
                        er_budsjetterbar,
                        er_aktiv,
                        er_siste_gyldige,
                        har_hierarki,
                        kostnadsstedsniva_5,
                        kostnadsstedsniva_5_beskrivelse,
                        kostnadsstedsniva_4,
                        kostnadsstedsniva_4_beskrivelse,
                        kostnadsstedsniva_3,
                        kostnadsstedsniva_3_beskrivelse,
                        kostnadsstedsniva_2,
                        kostnadsstedsniva_2_beskrivelse,
                        kostnadsstedsniva_1,
                        kostnadsstedsniva_1_beskrivelse,
                        kostnadssted_totalniva,
                        kostnadssted_totalniva_beskrivelse
                    from okonomimodell.marts.dim_kostnadssteder
                ),
                depricated as (
                    select
                        *,
                        kostnadssted as kostnadssteder_segment_kode,
                        kostnadssted_beskrivelse as kostnadssteder_segment_beskrivelse,
                        kostnadssted_totalniva as kostnadssteder_segment_kode_niva_0,
                        kostnadssted_totalniva_beskrivelse
                        as kostnadssteder_segment_beskrivelse_niva_0,
                        kostnadsstedsniva_1 as kostnadssteder_segment_kode_niva_1,
                        kostnadsstedsniva_1_beskrivelse
                        as kostnadssteder_segment_beskrivelse_niva_1,
                        kostnadsstedsniva_2 as kostnadssteder_segment_kode_niva_2,
                        kostnadsstedsniva_2_beskrivelse
                        as kostnadssteder_segment_beskrivelse_niva_2,
                        kostnadsstedsniva_3 as kostnadssteder_segment_kode_niva_3,
                        kostnadsstedsniva_3_beskrivelse
                        as kostnadssteder_segment_beskrivelse_niva_3,
                        kostnadsstedsniva_4 as kostnadssteder_segment_kode_niva_4,
                        kostnadsstedsniva_4_beskrivelse
                        as kostnadssteder_segment_beskrivelse_niva_4,
                        kostnadsstedsniva_5 as kostnadssteder_segment_kode_niva_5,
                        kostnadsstedsniva_5_beskrivelse
                        as kostnadssteder_segment_beskrivelse_niva_5,
                        har_hierarki as _har_hierarki
                    from src
                ),
                final as (select * from depricated)

            select *
            from final
        """,
        import_hierarchy_data={
            "id": "112000000043",
            "name": "Ksted Flat from dim_kostnadssteder_snowflake.csv",
        },
        import_module_data={
            "id": "112000000067",
            "name": "SYS 03.01 Organisasjonsstru from dim_kostnadssteder_snowflak",
        },
    )

    upload_produkter = transfer.override(task_id="transfer_produkter")(
        fileData={"id": "113000000031", "name": "dim_produkter_snowflake.csv"},
        query="""
            with
                src as (
                    select
                        pk_dim_produkter,
                        kode,
                        beskrivelse,
                        ar,
                        posterbar_fra_dato,
                        posterbar_til_dato,
                        er_summeringsniva,
                        er_posterbar,
                        er_budsjetterbar,
                        er_aktiv,
                        er_siste_gyldige,
                        har_hierarki,
                        produkt,
                        produkt_beskrivelse,
                        produktgruppe,
                        produktgruppe_beskrivelse,
                        produktkategori,
                        produktkategori_beskrivelse,
                        produkttype,
                        produkttype_beskrivelse,
                        produkt_totalniva,
                        produkt_totalniva_beskrivelse
                    from okonomimodell.marts.dim_produkter
                ),
                depricated as (
                    select
                        *,
                        beskrivelse as produkter_segment_beskrivelse,
                        produkt_beskrivelse as produkter_segment_beskrivelse_niva_4,
                        produktgruppe_beskrivelse as produkter_segment_beskrivelse_niva_3,
                        produktkategori_beskrivelse as produkter_segment_beskrivelse_niva_2,
                        produkttype_beskrivelse as produkter_segment_beskrivelse_niva_1,
                        produkt_totalniva_beskrivelse as produkter_segment_beskrivelse_niva_0,
                        kode as produkter_segment_kode,
                        produkt as produkter_segment_kode_niva_4,
                        produktgruppe as produkter_segment_kode_niva_3,
                        produktkategori as produkter_segment_kode_niva_2,
                        produkttype as produkter_segment_kode_niva_1,
                        produkt_totalniva as produkter_segment_kode_niva_0,
                        har_hierarki as _har_hierarki
                    from src
                ),
                final as (select * from depricated)

            select *
            from final
        """,
        import_hierarchy_data={
            "id": "112000000045",
            "name": "Produkt Flat from dim_produkter_snowflake.csv",
        },
        import_module_data={
            "id": "112000000068",
            "name": "SYS 04.01 Produkthierarki from dim_produkter_snowflake.csv",
        },
    )

    upload_oppgaver = transfer.override(task_id="transfer_oppgaver")(
        fileData={"id": "113000000032", "name": "dim_oppgaver_snowflake.csv"},
        query="""
            with
                src as (
                    select
                        pk_dim_oppgaver,
                        kode,
                        beskrivelse,
                        ar,
                        posterbar_fra_dato,
                        posterbar_til_dato,
                        er_summeringsniva,
                        er_posterbar,
                        er_budsjetterbar,
                        er_aktiv,
                        er_siste_gyldige,
                        har_hierarki,
                        oppgave,
                        oppgave_beskrivelse,
                        oppgaveniva_3,
                        oppgaveniva_3_beskrivelse,
                        oppgaveniva_2,
                        oppgaveniva_2_beskrivelse,
                        oppgaveniva_1,
                        oppgaveniva_1_beskrivelse,
                        finansieringskilde,
                        kategorisering,
                        produktomrade,
                        eierkostnadssted
                    from okonomimodell.marts.dim_oppgaver
                ),
                depricated as (
                    select
                        *,
                        eierkostnadssted as ansvarlig_kostnadssted,
                        produktomrade as hovedansvarlig,
                        beskrivelse as oppgaver_segment_beskrivelse,
                        oppgaveniva_1_beskrivelse as oppgaver_segment_beskrivelse_niva_0,
                        oppgaveniva_2_beskrivelse as oppgaver_segment_beskrivelse_niva_1,
                        oppgaveniva_3_beskrivelse as oppgaver_segment_beskrivelse_niva_2,
                        oppgave_beskrivelse as oppgaver_segment_beskrivelse_niva_3,
                        kode as oppgaver_segment_kode,
                        oppgaveniva_1 as oppgaver_segment_kode_niva_0,
                        oppgaveniva_2 as oppgaver_segment_kode_niva_1,
                        oppgaveniva_3 as oppgaver_segment_kode_niva_2,
                        oppgave as oppgaver_segment_kode_niva_3,
                        har_hierarki as _har_hierarki,
                    from src
                ),
                final as (select * from depricated)

            select *
            from final
        """,
        import_hierarchy_data={
            "id": "112000000047",
            "name": "Oppgave Flat from dim_oppgaver_snowflake.csv",
        },
        import_module_data={
            "id": "112000000069",
            "name": "SYS 05.01 Oppgave from dim_oppgaver_snowflake.csv",
        },
    )

    upload_felles = transfer.override(task_id="transfer_felles")(
        fileData={"id": "113000000033", "name": "dim_felles_snowflake.csv"},
        query="""
            with
                src as (
                    select
                        pk_dim_felles,
                        felles,
                        felles_beskrivelse,
                        ar,
                        posterbar_fra_dato,
                        posterbar_til_dato,
                        er_summeringsniva,
                        er_posterbar,
                        er_budsjetterbar,
                        er_aktiv,
                        er_siste_gyldige,
                        har_hierarki
                    from okonomimodell.marts.dim_felles
                ),
                depricated as (
                    select
                        *,
                        felles_beskrivelse as felles_segment_beskrivelse,
                        felles as felles_segment_kode,
                        har_hierarki as _har_hierarki
                    from src
                ),

                final as (select * from depricated)

            select *
            from final
        """,
        import_hierarchy_data={
            "id": "112000000049",
            "name": "Felles Flat from dim_felles_snowflake.csv",
        },
        import_module_data={
            "id": "112000000070",
            "name": "SYS 05.02 Felles from dim_felles_snowflake.csv",
        },
    )

    upload_statsregnskapskonti = transfer.override(
        task_id="transfer_statsregnskapskonti"
    )(
        fileData={
            "id": "113000000034",
            "name": "dim_statsregnskapskonti_snowflake.csv",
        },
        query="""
            with
                src as (
                    select
                        pk_dim_statsregnskapskonti,
                        kode,
                        beskrivelse,
                        ar,
                        posterbar_fra_dato,
                        posterbar_til_dato,
                        er_summeringsniva,
                        er_posterbar,
                        er_budsjetterbar,
                        er_aktiv,
                        er_siste_gyldige,
                        har_hierarki,
                        statsregnskapskonto,
                        statsregnskapskonto_beskrivelse,
                        post,
                        post_beskrivelse,
                        kapittel,
                        kapittel_beskrivelse,
                        statsregnskapskonto_totalniva,
                        statsregnskapskonto_totalniva_beskrivelse
                    from okonomimodell.marts.dim_statsregnskapskonti
                ),
                filter_unused as (
                    select *
                    from src
                    where
                        exists (
                            select 1
                            from reporting.microstrategy.mulige_kontostrenger kontostreng
                            where kontostreng.statsregnskapskonto = src.kode
                        )
                        or src.kode is null
                ),
                depricated as (
                    select
                        *,
                        kode as statsregnskapskonti_segment_kode,
                        beskrivelse as statsregnskapskonti_segment_beskrivelse,
                        statsregnskapskonto as statsregnskapskonti_segment_kode_niva_3,
                        statsregnskapskonto_beskrivelse
                        as statsregnskapskonti_segment_beskrivelse_niva_3,
                        post as statsregnskapskonti_segment_kode_niva_2,
                        post_beskrivelse as statsregnskapskonti_segment_beskrivelse_niva_2,
                        kapittel as statsregnskapskonti_segment_kode_niva_1,
                        kapittel_beskrivelse as statsregnskapskonti_segment_beskrivelse_niva_1,
                        statsregnskapskonto_totalniva as statsregnskapskonti_segment_kode_niva_0,
                        statsregnskapskonto_totalniva_beskrivelse
                        as statsregnskapskonti_segment_beskrivelse_niva_0,
                        right(post, 2) as statsregnskapskonti_segment_kode_post,
                        har_hierarki as _har_hierarki
                    from filter_unused
                ),
                final as (select * from depricated)

            select *
            from final
        """,
        import_hierarchy_data={
            "id": "112000000051",
            "name": "Statskonto Flat from dim_statsregnskapskonti_snowflake.csv",
        },
        import_module_data={
            "id": "112000000071",
            "name": "SYS 06.01 Statskontohierark from dim_statsregnskapskonti_sno",
        },
    )

    slack_notification = send_slack_notification()

    upload_artskonti >> slack_notification

    upload_kostnadssteder >> slack_notification

    upload_produkter >> slack_notification

    upload_oppgaver >> slack_notification

    upload_felles >> slack_notification

    upload_statsregnskapskonti >> slack_notification


anaplan_datahub_regnskaphierarkier()
