from datetime import datetime

from airflow.decorators import dag, task

from operators.slack_operator import slack_error, slack_success, slack_info


@dag(
    start_date=datetime(2023, 2, 14),
    schedule_interval=None,
    on_success_callback=slack_success,
    on_failure_callback=slack_error,
)
def hello_world():
    @task()
    def send_slack_message():
        slack_info(message="Hello, World!")

    slack_message = send_slack_message()

    slack_message


hello_world()
