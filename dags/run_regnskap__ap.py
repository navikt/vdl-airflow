from datetime import datetime
from airflow import DAG

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.sensors.base import PokeReturnValue
from kubernetes import client as k8s

from custom.operators.slack_operator import slack_error, slack_success
from custom.decorators import CUSTOM_IMAGE
from operators.elementary import elementary_operator

URL = Variable.get("VDL_REGNSKAP_URL")

with DAG(
    start_date=datetime(2023, 3, 18),
    schedule_interval="0 19 * * *",
    dag_id="regnskap__ap_dag",
    catchup=False,
    default_args={"on_failure_callback": slack_error, "retries": 3},
    max_active_runs=1,
) as dag:

    @task(
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    annotations={
                        "allowlist": ",".join(
                            [
                                "slack.com",
                                "vdl-regnskap.intern.nav.no",
                                "vdl-regnskap.dev.intern.nav.no",
                            ]
                        )
                    }
                ),
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            image=CUSTOM_IMAGE,
                            resources=k8s.V1ResourceRequirements(
                                requests={"ephemeral-storage": "100M"},
                                limits={"ephemeral-storage": "200M"},
                            ),
                        )
                    ]
                ),
            )
        },
    )
    def run_inbound_job(job_name: str) -> dict:
        import requests

        response: requests.Response = requests.get(url=f"{URL}/inbound/run/{job_name}")
        if response.status_code > 400:
            raise AirflowFailException(
                "inboundjobb eksisterer mest sannsynlig ikke på podden"
            )
        return response.json()

    @task.sensor(
        poke_interval=60,
        timeout=8 * 60 * 60,
        mode="reschedule",
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    annotations={
                        "allowlist": ",".join(
                            [
                                "slack.com",
                                "vdl-regnskap.intern.nav.no",
                                "vdl-regnskap.dev.intern.nav.no",
                            ]
                        )
                    }
                ),
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            image=CUSTOM_IMAGE,
                            resources=k8s.V1ResourceRequirements(
                                requests={"ephemeral-storage": "100M"},
                                limits={"ephemeral-storage": "200M"},
                            ),
                        )
                    ]
                ),
            )
        },
    )
    def check_status_for_inbound_job(job_id: dict) -> PokeReturnValue:
        import requests

        id = job_id.get("job_id")

        response: requests.Response = requests.get(url=f"{URL}/inbound/status/{id}")
        if response.status_code > 400:
            raise AirflowFailException(
                "inboundjobb eksisterer mest sannsynlig ikke på podden"
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

    accounts_payable_closed = run_inbound_job.override(
        task_id="start_accounts_payable_closed"
    )("accounts_payable_closed")
    wait_accounts_payable_closed = check_status_for_inbound_job(accounts_payable_closed)

    accounts_payable_closed >> wait_accounts_payable_closed
