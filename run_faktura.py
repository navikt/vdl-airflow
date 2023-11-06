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
    def run_inbound_job(action: str = None, job: str = None, job_id: str = None, callback: str = None) -> dict:
        import requests

        url = f"{URL}/run_job/"

        if action is not None:
            url = f"{url}?action={action}"

        if job is not None:
            url = f"{url}&job={job}"

        if job_id is not None:
            url = f"{url}&job_id={job_id}"

        if callback is not None:
            url = f"{url}&callback={callback}"

        print("request url: ", url)
        response: requests.Response = requests.get(url=url)
        if response.status_code > 400:
            raise AirflowFailException(
                f"url {url}. response: {response.status_code}. {response.reason}"
            )
        return response.json()
    
    @task.sensor(poke_interval=60, timeout=8 * 60 * 60, mode="reschedule")
    def check_status_for_inbound_job(job_id: dict) -> PokeReturnValue:
        import requests

        id = job_id.get("job_id")

        print("job id :", job_id)
        url=f"{URL}/job_results/{id}"
        print("job status url: ", url)

        response: requests.Response = requests.get(url=url)
        if response.status_code > 400:
            print("response :", response.status_code, ". ", response.reason)
            raise AirflowFailException(
                f"url {url}. response: {response.status_code}. {response.reason}"
            )
        response: dict = response.json()
        print("probe response ", response)

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