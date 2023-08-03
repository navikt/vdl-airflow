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
    def transfer(fileData: dict, query: str):
        from anaplan.singleChunkUpload import transfer_data
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from anaplan.get_data import get_data

        with SnowflakeHook().get_cursor() as cursor:
            data = get_data(query, cursor)

        transfer_data(wGuid, mGuid, username, password, fileData, data)

    @task
    def update_data(importData: dict):
        from anaplan.import_data import import_data

        import_data(wGuid, mGuid, username, password, importData)

    upload_artskonti = transfer.override(task_id="transfer_artskonti")(
        fileData={"id": "113000000033", "name": "dim_artskonti.csv"},
        query="""
                select * from reporting.microstrategy.dim_artskonti where ENDSWITH(artskonti_segment_kode, '0000000') and er_budsjetterbar=1
                """,
    )

    refresh_hierarchy_data_artskonti = update_data.override(
        task_id="update_hierarchy_artskonti"
    )(
        importData={
            "id": "112000000052",
            "name": "Test Artskonto Flat from dim_artskonti.csv",
        }
    )

    refresh_module_data_artskonti = update_data.override(
        task_id="update_module_artskonti"
    )(
        importData={
            "id": "112000000051",
            "name": "TEST 01.02 Test Kontostruktur 2 from dim_artskonti.csv",
        }
    )

    upload_felles = transfer.override(task_id="transfer_felles")(
        fileData={"id": "113000000034", "name": "dim_felles.csv"},
        query="""
                select *
                from reporting.microstrategy.dim_felles
                where
                    er_budsjetterbar = 1
                """,
    )

    refresh_hierarchy_data_felles = update_data.override(
        task_id="update_hierarchy_felles"
    )(importData={"id": "112000000053", "name": "Test Felles Flat from dim_felles.csv"})

    refresh_module_data_felles = update_data.override(task_id="update_module_felles")(
        importData={
            "id": "112000000054",
            "name": "TEST 01.02 Test Felles from dim_felles.csv",
        }
    )

    upload_kostnadssteder = transfer.override(task_id="transfer_kostnadssteder")(
        fileData={"id": "113000000035", "name": "dim_kostnadssteder.csv"},
        query="""
                select *
                from reporting.microstrategy.dim_kostnadssteder
                where
                    er_budsjetterbar = 1
                """,
    )

    refresh_hierarchy_data_kostnadssteder = update_data.override(
        task_id="update_hierarchy_kostnadssteder"
    )(
        importData={
            "id": "112000000055",
            "name": "Test Ksted Flat from dim_kostnadssteder.csv",
        }
    )

    refresh_module_data_kostnadssteder = update_data.override(
        task_id="update_module_kostnadssteder"
    )(
        importData={
            "id": "112000000056",
            "name": "TEST 01.04 Org.Struktur from dim_kostnadssteder.csv",
        }
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
    )

    refresh_hierarchy_data_oppgaver = update_data.override(
        task_id="update_hierarchy_oppgaver"
    )(
        importData={
            "id": "112000000057",
            "name": "Test Oppgave Flat from dim_oppgaver.csv",
        }
    )

    refresh_module_data_oppgaver = update_data.override(
        task_id="update_module_oppgaver"
    )(
        importData={
            "id": "112000000064",
            "name": "TEST 01.05 Oppgave from dim_oppgaver.csv",
        }
    )

    upload_produkter = transfer.override(task_id="transfer_produkter")(
        fileData={"id": "113000000037", "name": "dim_produkter.csv"},
        query="""
            select *
            from reporting.microstrategy.dim_produkter
            where
                er_budsjetterbar = 1
            """,
    )

    refresh_hierarchy_data_produkter = update_data.override(
        task_id="update_hierarchy_produkter"
    )(
        importData={
            "id": "112000000059",
            "name": "Test Produkt Flat from dim_produkter.csv",
        }
    )

    refresh_module_data_produkter = update_data.override(
        task_id="update_module_produkter"
    )(
        importData={
            "id": "112000000060",
            "name": "TEST 01.01 Test Produkt from dim_produkter.csv",
        }
    )

    upload_statsregnskapskonti = transfer.override(
        task_id="transfer_statsregnskapskonti"
    )(
        fileData={"id": "113000000038", "name": "dim_statsregnskapskonti.csv"},
        query="""
        select * from reporting.microstrategy.dim_statsregnskapskonti where ENDSWITH(statsregnskapskonti_segment_kode, '000000') and er_budsjetterbar=1
            """,
    )

    refresh_hierarchy_data_statsregnskapskonti = update_data.override(
        task_id="update_hierarchy_statsregnskapskonti"
    )(
        importData={
            "id": "112000000061",
            "name": "Test Statsregnskapskonto Fl from dim_statsregnskapskonti.csv",
        }
    )

    refresh_module_data_statsregnskapskonti = update_data.override(
        task_id="update_module_statsregnskapskonti"
    )(
        importData={
            "id": "112000000062",
            "name": "TEST 01.06 Statskonto from dim_statsregnskapskonti.csv",
        }
    )

    (
        upload_artskonti
        >> refresh_hierarchy_data_artskonti
        >> refresh_module_data_artskonti
    )

    (upload_felles >> refresh_hierarchy_data_felles >> refresh_module_data_felles)

    (
        upload_kostnadssteder
        >> refresh_hierarchy_data_kostnadssteder
        >> refresh_module_data_kostnadssteder
    )

    (upload_oppgaver >> refresh_hierarchy_data_oppgaver >> refresh_module_data_oppgaver)

    (
        upload_produkter
        >> refresh_hierarchy_data_produkter
        >> refresh_module_data_produkter
    )

    (
        upload_statsregnskapskonti
        >> refresh_hierarchy_data_statsregnskapskonti
        >> refresh_module_data_statsregnskapskonti
    )


anaplan_regnskaphierarkier()
