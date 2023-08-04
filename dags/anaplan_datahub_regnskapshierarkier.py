from datetime import datetime

from airflow.decorators import dag

from airflow.models import Variable

from custom.decorators import task

from custom.operators.slack_operator import slack_error, slack_success, slack_info


@dag(
    start_date=datetime(2023, 8, 2),
    schedule_interval=None,
    on_success_callback=slack_success,
    on_failure_callback=slack_error,
)
def anaplan_database_regnskaphierarkier():
    wGuid = "8a868cd985f53e7701860542f59e276e"
    mGuid = "06128127571046D7AA58504E98667194"
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
        fileData={"id": "113000000029", "name": "dim_artskonti_snowflake.csv"},
        query="""
                select * from reporting.microstrategy.dim_artskonti where ENDSWITH(artskonti_segment_kode, '0000000') and er_budsjetterbar=1
                """,
    )

    refresh_hierarchy_data_artskonti = update_data.override(
        task_id="update_hierarchy_artskonti"
    )(
        importData={
            "id": "112000000041",
            "name": "Artskonti Flat from dim_artskonti_snowflake.csv",
        }
    )

    refresh_module_data_artskonti = update_data.override(
        task_id="update_module_artskonti"
    )(
        importData={
            "id": "112000000042",
            "name": "Artskonti from dim_artskonti_snowflake.csv",
        }
    )
    upload_kostnadssteder = transfer.override(task_id="transfer_kostnadssteder")(
        fileData={"id": "113000000030", "name": "dim_kostnadssteder_snowflake.csv"},
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
            "id": "112000000043",
            "name": "Ksted Flat from dim_kostnadssteder_snowflake.csv",
        }
    )

    refresh_module_data_kostnadssteder = update_data.override(
        task_id="update_module_kostnadssteder"
    )(
        importData={
            "id": "112000000044",
            "name": "Ksted from dim_kostnadssteder_snowflake.csv",
        }
    )

    upload_produkter = transfer.override(task_id="transfer_produkter")(
        fileData={"id": "113000000031", "name": "dim_produkter_snowflake.csv"},
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
            "id": "112000000045",
            "name": "Produkt Flat from dim_produkter_snowflake.csv",
        }
    )

    refresh_module_data_produkter = update_data.override(
        task_id="update_module_produkter"
    )(
        importData={
            "id": "112000000046",
            "name": "Produkt from dim_produkter_snowflake.csv",
        }
    )

    upload_oppgaver = transfer.override(task_id="transfer_oppgaver")(
        fileData={"id": "113000000032", "name": "dim_oppgaver_snowflake.csv"},
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
            "id": "112000000047",
            "name": "Oppgave Flat from dim_oppgaver_snowflake.csv",
        }
    )

    refresh_module_data_oppgaver = update_data.override(
        task_id="update_module_oppgaver"
    )(
        importData={
            "id": "112000000048",
            "name": "Oppgave from dim_oppgaver_snowflake.csv",
        }
    )

    upload_felles = transfer.override(task_id="transfer_felles")(
        fileData={"id": "113000000033", "name": "dim_felles_snowflake.csv"},
        query="""
                select *
                from reporting.microstrategy.dim_felles
                where
                    er_budsjetterbar = 1
                """,
    )

    refresh_hierarchy_data_felles = update_data.override(
        task_id="update_hierarchy_felles"
    )(
        importData={
            "id": "112000000049",
            "name": "Felles Flat from dim_felles_snowflake.csv",
        }
    )

    refresh_module_data_felles = update_data.override(task_id="update_module_felles")(
        importData={
            "id": "112000000050",
            "name": "Felles from dim_felles_snowflake.csv",
        }
    )

    upload_statsregnskapskonti = transfer.override(
        task_id="transfer_statsregnskapskonti"
    )(
        fileData={
            "id": "113000000034",
            "name": "dim_statsregnskapskonti_snowflake.csv",
        },
        query="""
        select * from reporting.microstrategy.dim_statsregnskapskonti where ENDSWITH(statsregnskapskonti_segment_kode, '000000') and er_budsjetterbar=1
            """,
    )

    refresh_hierarchy_data_statsregnskapskonti = update_data.override(
        task_id="update_hierarchy_statsregnskapskonti"
    )(
        importData={
            "id": "112000000051",
            "name": "Statskonto Flat from dim_statsregnskapskonti_snowflake.csv",
        }
    )

    refresh_module_data_statsregnskapskonti = update_data.override(
        task_id="update_module_statsregnskapskonti"
    )(
        importData={
            "id": "112000000052",
            "name": "Statskonti from dim_statsregnskapskonti_snowflake.csv",
        }
    )

    (
        upload_artskonti
        >> refresh_hierarchy_data_artskonti
        >> refresh_module_data_artskonti
    )

    (
        upload_kostnadssteder
        >> refresh_hierarchy_data_kostnadssteder
        >> refresh_module_data_kostnadssteder
    )

    (
        upload_produkter
        >> refresh_hierarchy_data_produkter
        >> refresh_module_data_produkter
    )

    (upload_oppgaver >> refresh_hierarchy_data_oppgaver >> refresh_module_data_oppgaver)

    (upload_felles >> refresh_hierarchy_data_felles >> refresh_module_data_felles)

    (
        upload_statsregnskapskonti
        >> refresh_hierarchy_data_statsregnskapskonti
        >> refresh_module_data_statsregnskapskonti
    )


anaplan_database_regnskaphierarkier()
