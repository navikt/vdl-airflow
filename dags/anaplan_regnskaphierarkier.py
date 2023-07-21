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
    @task
    def transfer():
        from anaplan.singleChunkUpload import transfer_data
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from anaplan.get_data import get_data
        wGuid = "8a868cda860a533a0186334e91805794"
        mGuid = "A07AB2A8DBA24E13B8A6E9EBCDB6235E"
        username = "virksomhetsdatalaget@nav.no"
        password = Variable.get("anaplan_password")
        fileData = {
            "id": "113000000033",
            "name": "dim_artskonti.csv",
            "chunkCount": 1,
            "delimiter": '"',
            "encoding": "UTF-8",
            "firstDataRow": 2,
            "format": "txt",
            "headerRow": 1,
            "separator": ",",
        }

        with SnowflakeHook().get_cursor() as cursor:
            query =  """
                select *
                from reporting.microstrategy.dim_artskonti
                where
                    er_budsjetterbar = 1 and
                    artskonti_segment_kode_niva_1 is not null
                """
            data = get_data(query, cursor)

        transfer_data(wGuid, mGuid, username, password, fileData, data)

    upload = transfer()

    @task
    def update_hierarchy_data():
        from anaplan.regnskaphierarki.import_hierarchy import hierarchy_data

        hierarchy_data()

    refresh_hierarchy_data = update_hierarchy_data()

    @task
    def update_module_data():
        from anaplan.regnskaphierarki.import_module_data import module_data

        module_data()

    refresh_module_data = update_module_data()

    upload >> refresh_hierarchy_data >> refresh_module_data


anaplan_regnskaphierarkier()
