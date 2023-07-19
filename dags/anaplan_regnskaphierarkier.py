from datetime import datetime

from airflow.decorators import dag, task

from operators.slack_operator import slack_error, slack_success, slack_info
from kubernetes import client as k8s


@dag(
    start_date=datetime(2023, 7, 17),
    schedule_interval=None,
    on_success_callback=slack_success,
    on_failure_callback=slack_error,
)
def anaplan_regnskaphierarkier():
    @task(
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            image="ghcr.io/navikt/vdl-airflow:2739f712d781142f78e173f76bb0be31d17b94df",
                        )
                    ]
                )
            )
        }
    )
    def transfer():
        from anaplan.regnskaphierarki.singleChunkUpload import transfer_data
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        with SnowflakeHook().get_cursor() as cursor:
            cursor.execute("select 1 from dual")
            result = cursor.fetchone()
            print(result)

    upload = transfer()

    @task
    def update_hierarchy_data():
        from anaplan.regnskaphierarki.import_hierarchy import hierarchy_data

    refresh_hierarchy_data = update_hierarchy_data()

    @task
    def update_module_data():
        from anaplan.regnskaphierarki.import_module_data import module_data

    refresh_module_data = update_module_data()

    upload >> refresh_hierarchy_data >> refresh_module_data


anaplan_regnskaphierarkier()
