from datetime import datetime

from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.http.sensors.http import HttpSensor
from operators.slack_operator import slack_error, slack_info

URL = Variable.get("VDL_REGNSKAP_URL")


@dag(
    start_date=datetime(2023, 2, 28),
    schedule_interval=None,
    on_success_callback=slack_info,
    on_failure_callback=slack_error,
)
def run_regnskap():
    @task()
    def send_slack_message():
        slack_info(message="Jeg kjører ingest LoL!")

    @task()
    def ingest_dimensional_data():
        import requests

        return requests.get(url=f"{URL}/inbound/run/dimensional_data")

    def response_check(response, task_instance):
        # The task_instance is injected, so you can pull data form xcom
        # Other context variables such as dag, ds, execution_date are also available.
        xcom_data = task_instance.xcom_pull(task_ids="ingest_dimensional_data")
        # In practice you would do something more sensible with this data..
        print(xcom_data)
        print(response)
        return True

    job_id = ingest_dimensional_data()

    sensor = HttpSensor(
        task_id="my_http_sensor",
        http_conn_id="vdl-regnskap",
        endpoint=f"inbound/status/{job_id.job_id}",
        method="GET",
        request_params=dict(job_id=job_id),
        response_check=response_check,
    )

    # @task()
    # def ingest_ledger_open():
    #    import requests
    #
    #    requests.get(url=f"{URL}/inbound/run/ledger_open")
    #
    # @task()
    # def ingest_ledger_closed():
    #    import requests
    #
    #    requests.get(url=f"{URL}/inbound/run/ledger_open")

    slack_message = send_slack_message()

    # ledger_closed = ingest_ledger_closed()
    # ledger_open = ingest_ledger_open()

    slack_message >> job_id >> sensor
    # slack_message >> dimensional_data
    # slack_message >> ledger_closed
    # slack_message >> ledger_open


run_regnskap()
