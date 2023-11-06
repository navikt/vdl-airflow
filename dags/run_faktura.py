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
    def check_status_for_inbound_job(job_run_response: dict) -> PokeReturnValue:
        import requests

        id = job_run_response.get("job_id")

        print("job id :", id)
        url=f"{URL}/job_results/?job_id={id}"
        print("job status url: ", url)

        response: requests.Response = requests.get(url=url)
        if response.status_code > 400:
            print("response :", response.status_code, ". ", response.reason)
            raise AirflowFailException(
                f"url {url}. response: {response.status_code}. {response.reason}"
            )
        # TODO: Dette kan forenkles betydelig om vi henter global status i stedet for liste av jobber...
        job_results: dict = response.json()
        print("job results ", job_results)

        if not job_results:
            raise AirflowFailException(
                # TODO: link til GUI
                f"Ingen jobbresultater funnet for inbound jobb: {id}"
            )
        
        jobs_status = []
        for job_result in job_results:
            if job_result.get("success") == "True":
                jobs_status.append(True)
            else: 
                jobs_status.append(False)

        if all(jobs_status): # all succeeded?
            return PokeReturnValue(is_done=True)
        else:
            raise AirflowFailException(
                # TODO: link til GUI
                f"En eller flere inbound jobber har feilet! Sjekk loggene til podden for job: {id}"
            )

    row_counts = run_inbound_job(action="ingest", job="rowcounts.yml")
    wait_row_counts = check_status_for_inbound_job(row_counts)

    row_counts >> wait_row_counts

run_faktura()