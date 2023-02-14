from datetime import datetime
from airflow.decorators import dag, task

from operators.slack_operator import slack_info


@dag(start_date=datetime(2023, 2, 14), schedule_interval=None)
def hello_world():
    @task()
    def send_slack_message():
        slack_info("Hello, World!", channel="#virksomhetsdatalaget-info")

    send_slack_message()


hello_world()
