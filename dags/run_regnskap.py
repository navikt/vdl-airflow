from datetime import datetime

from airflow.models import Variable
from airflow.decorators import dag, task

from operators.slack_operator import slack_error, slack_info

URL = Variable.get("VDL_REGNSKAP_URL")


@dag(
    start_date=datetime(2023, 2, 28),
    schedule_interval=None,
    on_success_callback=slack_info,
    on_failure_callback=slack_error,
)
def run_regnskap():
    @task()
    def send_slack_message():
        slack_info(message="Jeg kjører ingest LoL!")

    @task()
    def ingest_dimensional_data() -> None:
        import requests

        res = requests.get(url=f"{URL}/inbound/run/dimensional_data")

    @task()
    def ingest_ledger_open():
        import requests

        requests.get(url=f"{URL}/inbound/run/ledger_open")

    @task()
    def ingest_ledger_closed():
        import requests

        requests.get(url=f"{URL}/inbound/run/ledger_open")

    slack_message = send_slack_message()
    dimensional_data = ingest_dimensional_data()
    ledger_closed = ingest_ledger_closed()
    ledger_open = ingest_ledger_open()

    slack_message >> dimensional_data >> ledger_closed >> ledger_open


run_regnskap()
