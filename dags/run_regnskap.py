from datetime import datetime

from airflow.models import Variable
from airflow.decorators import dag, task
from operators.slack_operator import slack_error, slack_info
from airflow.sensors.base import PokeReturnValue


URL = Variable.get("VDL_REGNSKAP_URL")


@dag(
    start_date=datetime(2023, 2, 28),
    schedule_interval="@daily",
    catchup=False,
    on_success_callback=slack_info,
    on_failure_callback=slack_error,
    default_args={"retries": 1},
)
def run_regnskap():
    @task()
    def send_slack_message():
        slack_info(message="Jeg kjører ingest LoL!")

    @task()
    def run_inbound_job(job_name: str) -> dict:
        import requests

        return requests.get(url=f"{URL}/inbound/run/{job_name}").json()

    @task.sensor(poke_interval=60, timeout=2 * 60 * 60, mode="reschedule")
    def wait_for_inbound_job(job_id: dict) -> PokeReturnValue:
        import requests

        id = job_id.get("job_id")

        response: dict = requests.get(url=f"{URL}/inbound/status/{id}").json()
        print(response)
        job_status = response.get("status")
        if job_status == "done":
            return PokeReturnValue(is_done=True)
        if job_status == "error":
            raise Exception("Lastejobben har feilet! Sjekk loggene til podden")

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

    slack_message = send_slack_message()

    @task()
    def run_dbt_job(job: str) -> dict:
        import requests

        return requests.get(url=f"{URL}/dbt/{job}").json()

    dbt_run = run_dbt_job.override(task_id="dbt_run")("run")
    dbt_test = run_dbt_job.override(task_id="dbt_test")("test")

    @task.sensor(poke_interval=60, timeout=2 * 60 * 60, mode="reschedule")
    def wait_for_dbt(job_status: dict) -> PokeReturnValue:
        import requests

        id = job_status.get("job_id")

        response: dict = requests.get(url=f"{URL}/dbt/status/{id}").json()
        print(response)
        job_status = response.get("status")
        if job_status == "done":
            return PokeReturnValue(is_done=True)
        if job_status == "error":
            raise Exception("Lastejobben har feilet! Sjekk loggene til podden")

    wait_dbt_run = wait_for_dbt.override(task_id="wait_for_dbt_run")(dbt_run)
    wait_dbt_test = wait_for_dbt.override(task_id="wait_for_dbt_test")(dbt_test)

    slack_message >> dimensonal_data >> wait_dimensonal_data
    slack_message >> sync_check >> wait_sync_check
    slack_message >> general_ledger_open >> wait_general_ledger_open
    slack_message >> general_ledger_budget >> wait_general_ledger_budget
    slack_message >> general_ledger_closed >> wait_general_ledger_closed
    slack_message >> balance_open >> wait_balance_open
    slack_message >> balance_budget >> wait_balance_budget
    slack_message >> balance_closed >> wait_balance_closed

    wait_dimensonal_data >> dbt_run
    wait_sync_check >> dbt_run
    wait_general_ledger_open >> dbt_run
    wait_general_ledger_budget >> dbt_run
    wait_general_ledger_closed >> dbt_run
    wait_balance_open >> dbt_run
    wait_balance_budget >> dbt_run
    wait_balance_closed >> dbt_run

    dbt_run >> wait_dbt_run >> dbt_test >> wait_dbt_test


run_regnskap()
