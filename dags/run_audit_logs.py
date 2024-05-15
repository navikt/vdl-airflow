from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from kubernetes import client as k8s

from custom.operators.slack_operator import slack_error
from custom.decorators import CUSTOM_IMAGE

URL = Variable.get("VDL_LOGGDATA_URL")


@dag(
    start_date=datetime(2024, 4, 16),
    schedule_interval="0 2 * * *",  # Hver natt
    catchup=False,
    default_args={"on_failure_callback": slack_error, "retries": 3},
    max_active_runs=1,
)
def run_audit_logs():
    @task(
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    annotations={
                        "allowlist": ",".join(
                            [
                                "slack.com",
                                "vdl-loggdata.intern.nav.no",
                            ]
                        )
                    }
                ),
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            image=CUSTOM_IMAGE,
                        )
                    ]
                ),
            )
        },
    )
    
    def run_inbound_job(job_name: str) -> dict:
        import requests

        print(job_name)

        response: requests.Response = requests.get(url=f"{URL}/inbound/run/{job_name}")
        if response.status_code > 400:
            raise AirflowFailException(
                "inboundjobb eksisterer mest sannsynlig ikke på podden"
            )
        return response.json()
    
    eyeshare = run_inbound_job.override(task_id="start_eyeshare")(
        "eyeshare"
    )

    anaplan = run_inbound_job.override(task_id="start_anaplan")(
        "anaplan"
    )

    eyeshare
    
    anaplan

run_audit_logs()