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

    upload_artskonti = transfer.override(task_id="transfer_artskonti")(
        fileData={
            "id": "113000000033",
            "name": "dim_artskonti.csv",
            "chunkCount": 1,
            "delimiter": '"',
            "encoding": "UTF-8",
            "firstDataRow": 2,
            "format": "txt",
            "headerRow": 1,
            "separator": ",",
        },
        query="""
                select *
                from reporting.microstrategy.dim_artskonti
                where
                    er_budsjetterbar = 1 and
                    artskonti_segment_kode_niva_1 is not null
                """,
    )

    @task
    def update_data(importData: dict):
        from anaplan.import_data import import_data

        import_data(wGuid, mGuid, username, password, importData)

    refresh_hierarchy_data_artskonti = update_data.override(
        task_id="update_hierarchy_artskonti"
    )(
        importData={
            "id": "112000000052",
            "name": "Test Artskonto Flat from dim_artskonti.csv",
            "importDataSourceId": "113000000033",
            "importType": "HIERARCHY_DATA",
        }
    )

    refresh_module_data_artskonti = update_data.override(
        task_id="update_module_artskonti"
    )(
        importData={
            "id": "112000000051",
            "name": "TEST 01.02 Test Kontostruktur 2 from dim_artskonti.csv",
            "importDataSourceId": "113000000033",
            "importType": "MODULE_DATA",
        }
    )

    upload_felles = transfer.override(task_id="transfer_felles")(
        fileData={
            "id": "113000000034",
            "name": "dim_felles.csv",
            "chunkCount": 1,
            "delimiter": '"',
            "encoding": "UTF-8",
            "firstDataRow": 2,
            "format": "txt",
            "headerRow": 1,
            "separator": ",",
        },
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
            "id": "112000000053",
            "name": "Test Felles Flat from dim_felles.csv",
            "importDataSourceId": "113000000034",
            "importType": "HIERARCHY_DATA",
        }
    )

    refresh_module_data_felles = update_data.override(task_id="update_module_felles")(
        importData={
            "id": "112000000054",
            "name": "TEST 01.02 Test Felles from dim_felles.csv",
            "importDataSourceId": "113000000034",
            "importType": "MODULE_DATA",
        }
    )

    (
        upload_artskonti
        >> refresh_hierarchy_data_artskonti
        >> refresh_module_data_artskonti
    )
    upload_felles >> refresh_hierarchy_data_felles >> refresh_module_data_felles


anaplan_regnskaphierarkier()
