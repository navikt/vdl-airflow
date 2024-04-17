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

URL = Variable.get("VDL_FAKTURA_URL")

with DAG(
    start_date=datetime(2023, 2, 28),
    #schedule_interval="@daily",
    schedule_interval="0 3 * * *",  # Hver dag klokken 03:00 UTC
    dag_id="faktura_dag_inbound",
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

        print(job_name)

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
            error_message = response["job_result"]["error_message"]
            raise AirflowFailException(
                f"Lastejobben har feilet! Sjekk loggene til podden. Feilmelding: {error_message}"
            )


    invoice = run_inbound_job.override(task_id="start_invoice")(
        "invoice"
    )
    wait_invoice = check_status_for_inbound_job(invoice)

    

    #sync_check = run_inbound_job.override(task_id="start_sync_check")("sync_check")
    #wait_sync_check = check_status_for_inbound_job(sync_check)

    

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
    def run_dbt_job(job: str) -> dict:
        import requests

        return requests.get(url=f"{URL}/dbt/{job}").json()

    #dbt_freshness = run_dbt_job.override(task_id="start_dbt_freshness")("freshness")
    #dbt_run = run_dbt_job.override(task_id="start_dbt_run")("run_dbt")

    @task.sensor(
        poke_interval=60,
        timeout=2 * 60 * 60,
        mode="reschedule",
        on_failure_callback=None,
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
    def wait_for_dbt(job_status: dict) -> PokeReturnValue:
        import requests

        id = job_status.get("job_id")
        try:
            response: dict = requests.get(url=f"{URL}/dbt/status/{id}").json()
        except Exception:
            slack_error()
            raise Exception("Lastejobben har feilet! Sjekk loggene til podden")
        print(response)
        job_status = response.get("status")
        if job_status == "error":
            error_message = response["job_result"]["error_message"]
            raise AirflowFailException(
                f"Lastejobben har feilet! Sjekk loggene til podden. Feilmelding: {error_message}"
            )
        if job_status != "done":
            return PokeReturnValue(is_done=False)

        job_result = response.get("job_result")
        if job_result["dbt_run_result"]["exception"]:
            slack_error(message=job_result["dbt_run_result"]["exception"])
            raise AirflowFailException(job_result["dbt_run_result"]["exception"])

        if not job_result["dbt_run_result"]["success"]:
            dbt_error_messages = [
                result["msg"]
                for result in job_result["dbt_log"]
                if result["level"] in ["warning", "error"]
            ]
            error_message = "\n".join(dbt_error_messages)
            slack_error(message=f"```\n{error_message}\n```")
            raise AirflowFailException(error_message)
        summary_messages = [
            result["msg"]
            for result in job_result["dbt_log"]
            if result["code"] == "E047"
        ]
        return PokeReturnValue(is_done=True, xcom_value=summary_messages)

    @task(
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    annotations={
                        "allowlist": ",".join(
                            [
                                "slack.com",
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
    def send_slack_summary(dbt_test, dbt_run):
        dbt_test_summary = "\n".join(dbt_test)
        dbt_run_summary = "\n".join(dbt_run)
        summary = f"dbt test:\n```\n{dbt_test_summary}\n```\ndbt run:\n```\n{dbt_run_summary}\n```"
        slack_success(message=f"Resultat fra kjøringen:\n{summary}")

    #wait_dbt_freshness = wait_for_dbt.override(
    #    task_id="check_status_for_dbt_freshness"
    #)(dbt_freshness)
    #wait_dbt_run = wait_for_dbt.override(task_id="check_status_for_dbt_run")(dbt_run)

    #slack_summary = send_slack_summary(dbt_test=wait_dbt_run, dbt_run=wait_dbt_run)

    faktura_elementary_report = elementary_operator(
        dag=dag,
        task_id="elementary_report",
        commands=["./run.sh", "report"],
        allowlist=[
            "slack.com",
            "files.slack.com",
            "wx23413.europe-west4.gcp.snowflakecomputing.com",
        ],
        extra_envs={
            "DB": "faktura_raw",
            "DB_ROLE": "faktura_transformer",
            "DB_WH": "faktura_transformer",
        },
    )

    invoice >> wait_invoice
    #wait_invoice >> faktura_elementary_report

    #sync_check >> wait_sync_check


    #wait_sync_check >> dbt_freshness


    #dbt_freshness >> wait_dbt_freshness >> dbt_run >> wait_dbt_run >> slack_summary
    #wait_dbt_run >> faktura_elementary_report_report

