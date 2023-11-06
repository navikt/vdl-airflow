from datetime import datetime

from airflow.models import Variable
from airflow.decorators import dag, task
from custom.operators.slack_operator import slack_error, slack_success
from airflow.sensors.base import PokeReturnValue
from airflow.exceptions import AirflowFailException


URL = Variable.get("VDL_FAKTURA_URL")

@dag(
    start_date=datetime(2023, 11, 1, 6),
    schedule_interval="@daily",
    catchup=False,
    default_args={"on_failure_callback": slack_error},
    max_active_runs=1,
)
def run_faktura():
    @task()
    def run_inbound_job(action: str = None, job: str = None, callback: str = None) -> dict:
        import requests

        response: requests.Response = requests.get(url=f"{URL}/run_job/?action={action}&job={job}&callback={callback}")
        if response.status_code > 400:
            raise AirflowFailException(
                "dbt job eksisterer mest sannsynlig ikke på podden"
            )
        return response.json()
    
    @task.sensor(poke_interval=60, timeout=8 * 60 * 60, mode="reschedule")
    def check_status_for_inbound_job(job_id: dict) -> PokeReturnValue:
        import requests

        id = job_id.get("job_id")

        response: requests.Response = requests.get(url=f"{URL}/job_result/{id}")
        if response.status_code > 400:
            raise AirflowFailException(
                "inbound job eksisterer mest sannsynlig ikke på podden"
            )
        response: dict = response.json()
        print(response)
        job_status = response.get("status")
        if job_status == "done":
            return PokeReturnValue(is_done=True)
        if job_status == "error":
            raise AirflowFailException(
                "Lastejobben har feilet! Sjekk loggene til podden"
            )

    row_counts = run_inbound_job(action="ingest", job="rowcounts.yml")
    wait_row_counts = check_status_for_inbound_job(row_counts)

    row_counts >> wait_row_counts

run_faktura()