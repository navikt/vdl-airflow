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

    (
        upload_artskonti
        >> refresh_hierarchy_data_artskonti
        >> refresh_module_data_artskonti
    )


anaplan_regnskaphierarkier()
