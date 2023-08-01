from datetime import datetime

from airflow.decorators import dag

from airflow.models import Variable

from custom.decorators import task

from custom.operators.slack_operator import slack_error, slack_success, slack_info


@dag(
    start_date=datetime(2023, 7, 17),
    schedule_interval=None,
    on_success_callback=slack_success,
    on_failure_callback=slack_error,
)
def anaplan_regnskaphierarkier():
    wGuid = "8a868cda860a533a0186334e91805794"
    mGuid = "A07AB2A8DBA24E13B8A6E9EBCDB6235E"
    username = "virksomhetsdatalaget@nav.no"
    password = Variable.get("anaplan_password")

    @task
    def transfer(
        fileData: dict, query: str, hierarchy_import_data: dict, model_import_data: dict
    ):
        from anaplan.singleChunkUpload import transfer_data
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from anaplan.get_data import get_data
        from anaplan.import_data import import_data

        with SnowflakeHook().get_cursor() as cursor:
            data = get_data(query, cursor)

        transfer_data(wGuid, mGuid, username, password, fileData, data)
        import_data(
            wGuid=wGuid,
            mGuid=mGuid,
            username=username,
            password=password,
            importData=hierarchy_import_data,
        ),
        import_data(
            wGuid=wGuid,
            mGuid=mGuid,
            username=username,
            password=password,
            importData=model_import_data,
        )

    upload_artskonti = transfer.override(task_id="transfer_artskonti")(
        fileData={"id": "113000000033", "name": "dim_artskonti.csv"},
        query="""
            select *
            from reporting.microstrategy.dim_artskonti
            where
                er_budsjetterbar = 1 and
                artskonti_segment_kode_niva_1 is not null
        """,
        hierarchy_import_data={
            "id": "112000000052",
            "name": "Test Artskonto Flat from dim_artskonti.csv",
        },
        model_import_data={
            "id": "112000000051",
            "name": "TEST 01.02 Test Kontostruktur 2 from dim_artskonti.csv",
        },
    )

    upload_felles = transfer.override(task_id="transfer_felles")(
        fileData={"id": "113000000034", "name": "dim_felles.csv"},
        query="""
            select *
            from reporting.microstrategy.dim_felles
            where
                er_budsjetterbar = 1
        """,
        hierarchy_import_data={
            "id": "112000000053",
            "name": "Test Felles Flat from dim_felles.csv",
        },
        model_import_data={
            "id": "112000000054",
            "name": "TEST 01.02 Test Felles from dim_felles.csv",
        },
    )

    upload_kostnadssteder = transfer.override(task_id="transfer_kostnadssteder")(
        fileData={"id": "113000000035", "name": "dim_kostnadssteder.csv"},
        query="""
            select *
            from reporting.microstrategy.dim_kostnadssteder
            where
                er_budsjetterbar = 1
        """,
        hierarchy_import_data={
            "id": "112000000055",
            "name": "Test Ksted Flat from dim_kostnadssteder.csv",
        },
        model_import_data={
            "id": "112000000056",
            "name": "TEST 01.04 Org.Struktur from dim_kostnadssteder.csv",
        },
    )

    upload_oppgaver = transfer.override(task_id="transfer_oppgaver")(
        fileData={"id": "113000000036", "name": "dim_oppgaver.csv"},
        query="""
            with

            statskonti as (
                select distinct
                    oppgaver_segment_kode
                    ,statsregnskapskonti_segment_kode
                from reporting.microstrategy.fak_hovedbok_posteringer
                where er_budsjett = 1
            )

            ,oppgaver as (
                select * from reporting.microstrategy.dim_oppgaver where er_budsjetterbar = 1
            )

            select
                oppgaver.*
                ,statskonti.statsregnskapskonti_segment_kode
                ,case
                    when statskonti.statsregnskapskonti_segment_kode is null then
                        oppgaver.oppgaver_segment_kode else
                        concat(oppgaver.oppgaver_segment_kode,'_',statskonti.statsregnskapskonti_segment_kode)
                end as pk_oppgaver_statskonti
            from oppgaver
            left join statskonti on
                statskonti.oppgaver_segment_kode = oppgaver.oppgaver_segment_kode
        """,
        hierarchy_import_data={
            "id": "112000000057",
            "name": "Test Oppgave Flat from dim_oppgaver.csv",
        },
        model_import_data={
            "id": "112000000064",
            "name": "TEST 01.05 Oppgave from dim_oppgaver.csv",
        },
    )

    upload_produkter = transfer.override(task_id="transfer_produkter")(
        fileData={"id": "113000000037", "name": "dim_produkter.csv"},
        query="""
            select *
            from reporting.microstrategy.dim_produkter
            where
                er_budsjetterbar = 1
        """,
        hierarchy_import_data={
            "id": "112000000059",
            "name": "Test Produkt Flat from dim_produkter.csv",
        },
        model_import_data={
            "id": "112000000060",
            "name": "TEST 01.01 Test Produkt from dim_produkter.csv",
        },
    )

    upload_statsregnskapskonti = transfer.override(
        task_id="transfer_statsregnskapskonti"
    )(
        fileData={"id": "113000000038", "name": "dim_statsregnskapskonti.csv"},
        query="""
            select *
            from reporting.microstrategy.dim_statsregnskapskonti
            where
                er_budsjetterbar = 1
        """,
        hierarchy_import_data={
            "id": "112000000061",
            "name": "Test Statsregnskapskonto Fl from dim_statsregnskapskonti.csv",
        },
        model_import_data={
            "id": "112000000062",
            "name": "TEST 01.06 Statskonto from dim_statsregnskapskonti.csv",
        },
    )

    upload_artskonti

    upload_felles

    upload_kostnadssteder

    upload_oppgaver

    upload_produkter

    upload_statsregnskapskonti


anaplan_regnskaphierarkier()
