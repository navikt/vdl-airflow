from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from kubernetes import client as k8s

from custom.operators.slack_operator import slack_error
from custom.decorators import CUSTOM_IMAGE

URL = Variable.get("VDL_FAKTURA_URL")


@dag(
    start_date=datetime(2024, 4, 16),
    schedule_interval="*/30 * * * *",  # Hver halvtime
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
                                "vdl-faktura.intern.nav.no",
                                "vdl-faktura.intern.dev.nav.no"
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
    def run_audit_logs_job():
        import requests

        url = f"{URL}/audit_logs"
        print(url)
        response: requests.Response = requests.post(url)
        if response.status_code > 400:
            print(response)
            print(response.text)
            raise AirflowFailException("Noe gikk galt")
    
    audit_logs = run_audit_logs_job()

    audit_logs

run_audit_logs()