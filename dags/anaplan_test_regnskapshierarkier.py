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
def anaplan_test_regnskaphierarkier():
    # wGuid = "8a868cd985f53e7701860542f59e276e"
    # mGuid = "06128127571046D7AA58504E98667194"
    # username = "virksomhetsdatalaget@nav.no"
    # password = Variable.get("anaplan_password")

    wGuid = Variable.get("anaplan_workspace_id")
    mGuid = Variable.get("anaplan_model_id")
    username = Variable.get("anaplan_username")
    password = Variable.get("anaplan_password")

    @task
    def transfer(
        fileData: dict,
        import_hierarchy_data: dict,
        import_module_data: dict,
        query: str = None,
        local_csv: str = None,
    ):
        from anaplan.singleChunkUpload import transfer_data
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from anaplan.get_data import get_data
        from anaplan.import_data import import_data

        if query:
            with SnowflakeHook().get_cursor() as cursor:
                data = get_data(query, cursor)

        if local_csv:
            data = open(local_csv, "r").read().encode("utf-8")

        transfer_data(wGuid, mGuid, username, password, fileData, data)
        import_data(wGuid, mGuid, username, password, import_hierarchy_data)
        import_data(wGuid, mGuid, username, password, import_module_data)

    upload_artskonti = transfer.override(task_id="transfer_artskonti_test")(
        fileData={"id": "113000000038", "name": "artskonti_encoding_test.csv"},
        import_hierarchy_data={
            "id": "112000000059",
            "name": "TEST encoding artskonti from artskonti_encoding_test.csv",
        },
        import_module_data={
            "id": "112000000060",
            "name": "TEST encoding from artskonti_encoding_test.csv",
        },
        # local_csv="/Users/rubypaloma/Desktop/Anaplan/Python scripts/API/artskonti_encoding_test.csv",
        query="""
            select *
            from reporting.microstrategy.dim_artskonti
            where
                endswith(artskonti_segment_kode, '0000000') and
                er_budsjetterbar=1
        """,
    )

    (upload_artskonti)


anaplan_test_regnskaphierarkier()
