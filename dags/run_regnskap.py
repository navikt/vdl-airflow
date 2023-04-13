from datetime import datetime

from airflow.models import Variable
from airflow.decorators import dag, task
from operators.slack_operator import slack_error, slack_info
from airflow.sensors.base import PokeReturnValue


URL = Variable.get("VDL_REGNSKAP_URL")


@dag(
    start_date=datetime(2023, 2, 28),
    schedule_interval=None,
    on_success_callback=slack_info,
    on_failure_callback=slack_error,
    default_args={"retries": 1},
)
def run_regnskap():
    @task()
    def send_slack_message():
        slack_info(message="Jeg kjører ingest LoL!")

    @task()
    def ingest_dimensional_data() -> dict:
        import requests

        return requests.get(url=f"{URL}/inbound/run/dimensional_data").json()

    job_id = ingest_dimensional_data()

    @task.sensor(poke_interval=60, timeout=3600, mode="reschedule")
    def wait_for_upstream(job_id: dict) -> PokeReturnValue:
        import requests

        id = job_id.get("job_id")
        response: dict = requests.get(url=f"{URL}/inbound/status/{id}").json()
        print(response)
        job_status = response.get("status")
        print(job_status)
        if job_status == "running":
            print("inside running")
            return PokeReturnValue(is_done=False)
        if job_status ==  "done":
            print("inside done")
            return PokeReturnValue(is_done=True)
        if job_status == "error":
            print("inside error")
            raise Exception("Lastejobben har feilet! Sjekk loggene til podden")

    wait = wait_for_upstream(job_id)
    # @task()
    # def ingest_ledger_open():
    #    import requests
    #
    #    requests.get(url=f"{URL}/inbound/run/ledger_open")
    #
    # @task()
    # def ingest_ledger_closed():
    #    import requests
    #
    #    requests.get(url=f"{URL}/inbound/run/ledger_open")

    slack_message = send_slack_message()

    # ledger_closed = ingest_ledger_closed()
    # ledger_open = ingest_ledger_open()

    slack_message >> job_id >> wait
    # slack_message >> dimensional_data
    # slack_message >> ledger_closed
    # slack_message >> ledger_open


run_regnskap()
