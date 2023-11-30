from datetime import datetime

from airflow.models import Variable
from airflow.decorators import dag, task
from custom.operators.slack_operator import slack_error, slack_success
from airflow.sensors.base import PokeReturnValue
from airflow.exceptions import AirflowFailException

from kubernetes import client as k8s


URL = Variable.get("VDL_FAKTURA_URL")


@dag(
    start_date=datetime(2023, 11, 1, 6),
    schedule_interval="0 4 * * *",  # Hver dag klokken 04:00 UTC
    catchup=False,
    default_args={"on_failure_callback": slack_error},
    max_active_runs=1,
)
def run_faktura():
    @task(
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    annotations={
                        "allowlist": ",".join(
                            [
                                "slack.com",
                                "vdl-faktura.intern.nav.no",
                                "vdl-faktura.intern.dev.nav.no",
                            ]
                        )
                    }
                ),
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            image="europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/vdl-airflow@sha256:5edb4e907c93ee521f5f743c3b4346b1bae26721820a2f7e8dfbf464bf4c82ba",
                        )
                    ]
                ),
            )
        },
    )
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
                                "vdl-faktura.intern.nav.no",
                                "vdl-faktura.intern.dev.nav.no",
                            ]
                        )
                    }
                ),
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            image="europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/vdl-airflow@sha256:5edb4e907c93ee521f5f743c3b4346b1bae26721820a2f7e8dfbf464bf4c82ba",
                        )
                    ]
                ),
            )
        },
    )
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
        response: list[dict] = response.json()
        print(response)
        for res in response:
            if res.get("success") == "True":
                return PokeReturnValue(is_done=True)
            else:
                raise AirflowFailException(
                    "Lastejobben har feilet! Sjekk loggene til podden"
                )

    @task(
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    annotations={
                        "allowlist": ",".join(
                            [
                                "slack.com",
                                "vdl-faktura.intern.nav.no",
                                "vdl-faktura.intern.dev.nav.no",
                            ]
                        )
                    }
                ),
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            image="europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/vdl-airflow@sha256:5edb4e907c93ee521f5f743c3b4346b1bae26721820a2f7e8dfbf464bf4c82ba",
                        )
                    ]
                ),
            )
        },
    )
    def run_elementary(action: str) -> dict:
        import requests

        url = f"{URL}/elementary/{action}"
        response: requests.Response = requests.get(url)
        if response.status_code > 400:
            print(response)
            print(response.text)
            raise AirflowFailException("elementary har feilet")
        return response.json()

    ingest = run_inbound_job(action="ingest")
    wait_for_ingest = check_status_for_inbound_job(ingest)
    freshness = run_inbound_job(action="freshness")
    wait_for_freshness = check_status_for_inbound_job(freshness)
    transform = run_inbound_job(action="transform")
    wait_for_transform = check_status_for_inbound_job(transform)
    send_alert_to_slack = run_elementary(action="alert")
    send_report_to_slack = run_elementary(action="report")

    (
        ingest
        >> wait_for_ingest
        >> freshness
        >> wait_for_freshness
        >> transform
        >> wait_for_transform
        >> send_alert_to_slack
        >> send_report_to_slack
    )


run_faktura()
