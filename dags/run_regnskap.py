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
    def run_inbound_job(job_name: str) -> dict:
        import requests

        return requests.get(url=f"{URL}/inbound/run/{job_name}").json()

    

    @task.sensor(poke_interval=60, timeout=3600, mode="reschedule")
    def wait_for_inbound_job(job_id: dict) -> PokeReturnValue:
        import requests

        id = job_id.get("job_id")

        response: dict = requests.get(url=f"{URL}/inbound/status/{id}").json()
        print(response)
        job_status = response.get("status")

        if job_status == "running":
            return PokeReturnValue(is_done=False)
        if job_status ==  "done":
            return PokeReturnValue(is_done=True)
        if job_status == "error":
            raise Exception("Lastejobben har feilet! Sjekk loggene til podden")

    dimensonal_data = run_inbound_job("dimensional_data")
    wait_dimensonal_data = wait_for_inbound_job(dimensonal_data)

    general_ledger_closed = run_inbound_job("general_ledger_closed")
    wait_general_ledger_closed = wait_for_inbound_job(general_ledger_closed)

    balance_closed = run_inbound_job("balance_closed")
    wait_balance_closed = wait_for_inbound_job(balance_closed)
    
    #general_ledger_budget = run_inbound_job("general_ledger_budget")
    #wait_general_ledger_budget = wait_for_inbound_job(general_ledger_budget)

    slack_message = send_slack_message()

    slack_message >> dimensonal_data >> wait_dimensonal_data

    slack_message >> general_ledger_closed >> wait_general_ledger_closed

    slack_message >> balance_closed >> wait_balance_closed

    # slack_message >> dimensional_data
    # slack_message >> ledger_closed
    # slack_message >> ledger_open


run_regnskap()
