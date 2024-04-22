from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.sensors.base import PokeReturnValue
from kubernetes import client as k8s

from custom.operators.slack_operator import slack_error, slack_success
from custom.decorators import CUSTOM_IMAGE

URL = Variable.get("VDL_FAKTURA_URL")


@dag(
    start_date=datetime(2023, 11, 1, 6),
    schedule_interval="0 3 * * *",  # Hver dag klokken 03:00 UTC
    catchup=False,
    default_args={"on_failure_callback": slack_error, "retries": 3},
    max_active_runs=1,
)
def run_faktura_v2():
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
                            image=CUSTOM_IMAGE,
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
                            image=CUSTOM_IMAGE,
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
                            image=CUSTOM_IMAGE,
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
                            image=CUSTOM_IMAGE,
                        )
                    ]
                ),
            )
        },
    )
    def run_test_inbound(job_name: str) -> dict:
        import requests

        url = f"{URL}/inbound/run/{job_name}"
        print(url)
        response: requests.Response = requests.post(url)
        if response.status_code > 400:
            print(response)
            print(response.text)
            raise AirflowFailException("noe gikk galt")
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
                            image=CUSTOM_IMAGE,
                        )
                    ]
                ),
            )
        },
    )
    def check_status_for_test_inbound(job: dict) -> PokeReturnValue:
        import requests

        id = job.get("job_id")
        url = f"{URL}/inbound/status/{id}"
        print(url)
        response: requests.Response = requests.get(url=url)
        if response.status_code > 400:
            print(response)
            print(response.text)
            raise AirflowFailException(
                "test inbound job eksisterer mest sannsynlig ikke"
            )
        response: dict = response.json()
        print(response)
        status = response.get("status")
        if status == "finnished":
            return PokeReturnValue(is_done=True)
        if status == "failed":
            raise AirflowFailException(
                "Lastejobben har feilet! Sjekk loggene til podden"
            )

    inbound = run_test_inbound()
    wait_for_inbound = check_status_for_test_inbound(inbound)
    freshness = run_inbound_job(action="freshness")
    wait_for_freshness = check_status_for_inbound_job(freshness)
    transform = run_inbound_job(action="transform")
    wait_for_transform = check_status_for_inbound_job(transform)
    send_alert_to_slack = run_elementary(action="alert")
    send_report_to_slack = run_elementary(action="report")

    (
        inbound
        >> wait_for_inbound
        >> freshness
        >> wait_for_freshness
        >> transform
        >> wait_for_transform
        >> send_alert_to_slack
        >> send_report_to_slack
    )


run_faktura_v2()
