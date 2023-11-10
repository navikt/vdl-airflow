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
    def run_inbound_job(
        action: str = None, job: str = None, callback: str = None
    ) -> dict:
        import requests

        # url = f"{URL}/run_job/?action={action}&job={job}&callback={callback}"
        url = f"{URL}/run_job/?action={action}"
        print(url)
        response: requests.Response = requests.get(url)
        if response.status_code > 400:
            print(response)
            print(response.text)
            raise AirflowFailException(
                "dbt job eksisterer mest sannsynlig ikke på podden"
            )
        return response.json()

    # TODO: Denne kan droppes når vi finner ut hvorfor dbt forsøker å opprette et dbt_packages dir og får permission denied når dbt kjøres med dbtrunner
    @task()
    def run_dbt_job() -> dict:
        import requests

        url = f"{URL}/run_dbt"

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
        url = f"{URL}/job_results?job_id={id}"
        print(url)
        response: requests.Response = requests.get(url=url)
        if response.status_code > 400:
            print(response)
            print(response.text)
            raise AirflowFailException(
                "inbound job eksisterer mest sannsynlig ikke på podden"
            )
        response: dict = response.json()
        print(response)
        for itms in response: 
            if itms.ge('success')==True:
                return PokeReturnValue(is_done=True)
            else:
                raise AirflowFailException(
                    "Lastejobben har feilet! Sjekk loggene til podden"
                )

    ingest = run_inbound_job(action="ingest")
    wait_for_ingest = check_status_for_inbound_job(ingest)
    transform = run_dbt_job()

    ingest >> wait_for_ingest >> transform


run_faktura()
