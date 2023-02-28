from datetime import datetime

from airflow.decorators import dag, task

from operators.slack_operator import slack_error, slack_info


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
    def ingest_dimensional_data() -> None:
        import requests
        res = requests.get(url="https://vdl-regnskap.dev.intern.nav.no/inbound/run/dimensional_data")
        
    send_slack_message()
    ingest_dimensional_data()

run_regnskap()


