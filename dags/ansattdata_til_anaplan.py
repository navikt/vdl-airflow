from datetime import datetime

from airflow.decorators import dag, task

from operators.slack_operator import slack_error, slack_success, slack_info


@dag(
    start_date=datetime(2023, 7, 17),
    schedule_interval=None,
    on_success_callback=slack_success,
    on_failure_callback=slack_error,
)
def ansattdata_til_anaplan():
    @task()
    def transfer():
        from anaplan.singleChunkUpload import transfer_data

        transfer_data()

    upload = transfer()

    upload


ansattdata_til_anaplan()
