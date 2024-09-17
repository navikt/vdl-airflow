from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.sensors.base import PokeReturnValue
from kubernetes import client as k8s

from custom.decorators import CUSTOM_IMAGE
from custom.operators.slack_operator import slack_error, slack_success_old
from operators.elementary import elementary_operator

URL = Variable.get("VDL_REGNSKAP_URL")


with DAG(
    start_date=datetime(2023, 2, 28),
    schedule_interval="@daily",
    dag_id="run_regnskap",
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
                                "vdl-regnskap.intern.dev.nav.no",
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
                                "vdl-regnskap.intern.dev.nav.no",
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

    # dimensonal_data = run_inbound_job.override(task_id="start_dimensional_data")(
    #    "dimensional_data"
    # )
    # wait_dimensonal_data = check_status_for_inbound_job(dimensonal_data)
    segment = run_inbound_job.override(task_id="start_segment")("segment")
    wait_segment = check_status_for_inbound_job(segment)

    period_status = run_inbound_job.override(task_id="start_period_status")(
        "period_status"
    )
    wait_period_status = check_status_for_inbound_job(period_status)

    hierarchy = run_inbound_job.override(task_id="start_hierarchy")("hierarchy")
    wait_hierarchy = check_status_for_inbound_job(hierarchy)

    sync_check = run_inbound_job.override(task_id="start_sync_check")("sync_check")
    wait_sync_check = check_status_for_inbound_job(sync_check)

    general_ledger_closed = run_inbound_job.override(
        task_id="start_general_ledger_closed"
    )("general_ledger_closed")
    wait_general_ledger_closed = check_status_for_inbound_job(general_ledger_closed)

    general_ledger_open = run_inbound_job.override(task_id="start_general_ledger_open")(
        "general_ledger_open"
    )
    wait_general_ledger_open = check_status_for_inbound_job(general_ledger_open)

    balance_closed = run_inbound_job.override(task_id="start_balance_closed")(
        "balance_closed"
    )
    wait_balance_closed = check_status_for_inbound_job(balance_closed)

    balance_open = run_inbound_job.override(task_id="start_balance_open")(
        "balance_open"
    )
    wait_balance_open = check_status_for_inbound_job(balance_open)

    suppliers = run_inbound_job.override(task_id="start_suppliers")("suppliers")
    wait_suppliers = check_status_for_inbound_job(suppliers)

    accounts_payable_open = run_inbound_job.override(
        task_id="start_accounts_payable_open"
    )("accounts_payable_open")
    wait_accounts_payable_open = check_status_for_inbound_job(accounts_payable_open)

    accounts_payable_closed = run_inbound_job.override(
        task_id="start_accounts_payable_closed"
    )("accounts_payable_closed")
    wait_accounts_payable_closed = check_status_for_inbound_job(accounts_payable_closed)

    accounts_receivable_open = run_inbound_job.override(
        task_id="start_accounts_receivable_open"
    )("accounts_receivable_open")
    wait_accounts_receivable_open = check_status_for_inbound_job(
        accounts_receivable_open
    )

    accounts_receivable_closed = run_inbound_job.override(
        task_id="start_accounts_receivable_closed"
    )("accounts_receivable_closed")
    wait_accounts_receivable_closed = check_status_for_inbound_job(
        accounts_receivable_closed
    )

    budget = run_inbound_job.override(task_id="start_budget")("budget")
    wait_budget = check_status_for_inbound_job(budget)

    prognosis = run_inbound_job.override(task_id="start_prognosis")("prognosis")
    wait_prognosis = check_status_for_inbound_job(prognosis)

    customers = run_inbound_job.override(task_id="start_customers")("customers")
    wait_customers = check_status_for_inbound_job(customers)

    @task(
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    annotations={
                        "allowlist": ",".join(
                            [
                                "slack.com",
                                "vdl-regnskap.intern.nav.no",
                                "vdl-regnskap.intern.dev.nav.no",
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

    dbt_freshness = run_dbt_job.override(task_id="start_dbt_freshness")("freshness")
    dbt_run = run_dbt_job.override(task_id="start_dbt_run")("build")

    @task.sensor(
        poke_interval=60,
        timeout=2 * 60 * 60,
        mode="reschedule",
        on_failure_callback=None,
        outlets=[Dataset("regnskap_dataset")],
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    annotations={
                        "allowlist": ",".join(
                            [
                                "slack.com",
                                "vdl-regnskap.intern.nav.no",
                                "vdl-regnskap.intern.dev.nav.no",
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
        slack_success_old(message=f"Resultat fra kjøringen:\n{summary}")

    wait_dbt_freshness = wait_for_dbt.override(
        task_id="check_status_for_dbt_freshness"
    )(dbt_freshness)
    wait_dbt_run = wait_for_dbt.override(task_id="check_status_for_dbt_run")(dbt_run)

    slack_summary = send_slack_summary(dbt_test=wait_dbt_run, dbt_run=wait_dbt_run)

    regnskap_report = elementary_operator(
        dag=dag,
        task_id="regnskap_report",
        commands=["./run.sh", "report"],
        allowlist=[
            "slack.com",
            "files.slack.com",
            "wx23413.europe-west4.gcp.snowflakecomputing.com",
        ],
        extra_envs={
            "DB": "regnskap",
            "DB_ROLE": "regnskap_transformer",
            "DB_WH": "regnskap_transformer",
        },
    )

    period_status >> wait_period_status
    wait_period_status >> general_ledger_closed
    wait_period_status >> balance_closed
    wait_period_status >> accounts_payable_closed
    wait_period_status >> accounts_receivable_closed

    sync_check >> wait_sync_check

    general_ledger_closed >> wait_general_ledger_closed
    general_ledger_open >> wait_general_ledger_open

    balance_open >> wait_balance_open
    balance_closed >> wait_balance_closed

    accounts_payable_closed >> wait_accounts_payable_closed
    accounts_payable_open >> wait_accounts_payable_open

    accounts_payable_closed >> wait_accounts_receivable_closed
    accounts_receivable_open >> wait_accounts_receivable_open

    suppliers >> wait_suppliers
    segment >> wait_segment
    hierarchy >> wait_hierarchy
    budget >> wait_budget
    prognosis >> wait_prognosis
    customers >> wait_customers

    wait_sync_check >> dbt_freshness
    wait_general_ledger_open >> dbt_freshness
    wait_general_ledger_closed >> dbt_freshness
    wait_balance_open >> dbt_freshness
    wait_balance_closed >> dbt_freshness
    wait_suppliers >> dbt_freshness
    wait_hierarchy >> dbt_freshness
    wait_segment >> dbt_freshness
    wait_accounts_payable_open >> dbt_freshness
    wait_accounts_payable_closed >> dbt_freshness
    wait_accounts_receivable_open >> dbt_freshness
    wait_accounts_receivable_closed >> dbt_freshness
    wait_budget >> dbt_freshness
    wait_prognosis >> dbt_freshness
    wait_customers >> dbt_freshness

    dbt_freshness >> wait_dbt_freshness >> dbt_run >> wait_dbt_run >> slack_summary
    wait_dbt_run >> regnskap_report
