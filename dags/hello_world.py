from datetime import datetime

from airflow.decorators import dag, task

from operators.slack_operator import slack_error, slack_info


@dag(
    start_date=datetime(2023, 2, 14),
    schedule_interval=None,
    on_success_callback=slack_info,
    on_failure_callback=slack_error,
)
def hello_world():
    @task(on_success_callback=slack_info)
    def send_slack_message():
        slack_info(message="Hello, World!", channel="#virksomhetsdatalaget-info")

    send_slack_message()


hello_world()


