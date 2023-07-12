from datetime import datetime

from airflow.models import Variable
from airflow.decorators import dag, task
from operators.slack_operator import slack_error, slack_success
from airflow.sensors.base import PokeReturnValue
from airflow.exceptions import AirflowFailException


URL = Variable.get("VDL_REGNSKAP_URL")


@dag(
    start_date=datetime(2023, 2, 28),
    schedule_interval="@daily",
    catchup=False,
    default_args={"on_failure_callback": slack_error},
    max_active_runs=1,
)
def run_regnskap():
    @task()
    def run_inbound_job(job_name: str) -> dict:
        import requests

        return requests.get(url=f"{URL}/inbound/run/{job_name}").json()

    @task.sensor(poke_interval=60, timeout=8 * 60 * 60, mode="reschedule")
    def wait_for_inbound_job(job_id: dict) -> PokeReturnValue:
        import requests

        id = job_id.get("job_id")

        response: dict = requests.get(url=f"{URL}/inbound/status/{id}").json()
        print(response)
        job_status = response.get("status")
        if job_status == "done":
            return PokeReturnValue(is_done=True)
        if job_status == "error":
            raise AirflowFailException(
                "Lastejobben har feilet! Sjekk loggene til podden"
            )

    dimensonal_data = run_inbound_job.override(task_id="dimensional_data")(
        "dimensional_data"
    )
    wait_dimensonal_data = wait_for_inbound_job(dimensonal_data)

    sync_check = run_inbound_job.override(task_id="sync_check")("sync_check")
    wait_sync_check = wait_for_inbound_job(sync_check)

    general_ledger_closed = run_inbound_job.override(task_id="general_ledger_closed")(
        "general_ledger_closed"
    )
    wait_general_ledger_closed = wait_for_inbound_job(general_ledger_closed)

    general_ledger_open = run_inbound_job.override(task_id="general_ledger_open")(
        "general_ledger_open"
    )
    wait_general_ledger_open = wait_for_inbound_job(general_ledger_open)

    general_ledger_budget = run_inbound_job.override(task_id="general_ledger_budget")(
        "general_ledger_budget"
    )
    wait_general_ledger_budget = wait_for_inbound_job(general_ledger_budget)

    balance_closed = run_inbound_job.override(task_id="balance_closed")(
        "balance_closed"
    )
    wait_balance_closed = wait_for_inbound_job(balance_closed)

    balance_open = run_inbound_job.override(task_id="balance_open")("balance_open")
    wait_balance_open = wait_for_inbound_job(balance_open)

    balance_budget = run_inbound_job.override(task_id="balance_budget")(
        "balance_budget"
    )
    wait_balance_budget = wait_for_inbound_job(balance_budget)

    accounts_payable = run_inbound_job.override(task_id="accounts_payable")(
        "accounts_payable"
    )
    wait_accounts_payable = wait_for_inbound_job(accounts_payable)

    @task()
    def run_dbt_job(job: str) -> dict:
        import requests

        return requests.get(url=f"{URL}/dbt/{job}").json()

    dbt_run = run_dbt_job.override(task_id="dbt_run")("run")
    dbt_test = run_dbt_job.override(task_id="dbt_test")("test")

    @task.sensor(
        poke_interval=60,
        timeout=2 * 60 * 60,
        mode="reschedule",
        on_failure_callback=None,
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
            raise AirflowFailException(
                "Lastejobben har feilet! Sjekk loggene til podden"
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

    @task
    def send_slack_summary(dbt_test, dbt_run):
        dbt_test_summary = "\n".join(dbt_test)
        dbt_run_summary = "\n".join(dbt_run)
        summary = f"dbt test:\n```\n{dbt_test_summary}\n```\ndbt run:\n```\n{dbt_run_summary}\n```"
        slack_success(message=f"Resultat fra kjøringen:\n{summary}")

    wait_dbt_run = wait_for_dbt.override(task_id="wait_for_dbt_run")(dbt_run)
    wait_dbt_test = wait_for_dbt.override(task_id="wait_for_dbt_test")(dbt_test)

    slack_summary = send_slack_summary(dbt_test=wait_dbt_test, dbt_run=wait_dbt_run)

    dimensonal_data >> wait_dimensonal_data
    sync_check >> wait_sync_check
    general_ledger_open >> wait_general_ledger_open
    general_ledger_budget >> wait_general_ledger_budget
    general_ledger_closed >> wait_general_ledger_closed
    balance_open >> wait_balance_open
    balance_budget >> wait_balance_budget
    balance_closed >> wait_balance_closed
    accounts_payable >> wait_accounts_payable

    wait_dimensonal_data >> dbt_run
    wait_sync_check >> dbt_run
    wait_general_ledger_open >> dbt_run
    wait_general_ledger_budget >> dbt_run
    wait_general_ledger_closed >> dbt_run
    wait_balance_open >> dbt_run
    wait_balance_budget >> dbt_run
    wait_balance_closed >> dbt_run
    wait_accounts_payable >> dbt_run

    dbt_run >> wait_dbt_run >> dbt_test >> wait_dbt_test >> slack_summary


run_regnskap()
