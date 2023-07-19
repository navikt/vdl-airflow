from datetime import datetime

from airflow.decorators import dag, task

from operators.slack_operator import slack_error, slack_success, slack_info


@dag(
    start_date=datetime(2023, 7, 17),
    schedule_interval=None,
    on_success_callback=slack_success,
    on_failure_callback=slack_error,
)
def anaplan_ansattdata():
    @task
    def transfer():
        from anaplan.singleChunkUpload import transfer_data

        transfer_data()

    upload = transfer()

    @task
    def update_hierarchy_data():
        from anaplan.import_hierarchy import hierarchy_data

        hierarchy_data()

    refresh_hierarchy_data = update_hierarchy_data()

    @task
    def update_module_data():
        from anaplan.import_module_data import module_data

        module_data()

    refresh_module_data = update_module_data()

    upload >> refresh_hierarchy_data >> refresh_module_data


anaplan_ansattdata()
