from datetime import datetime

import requests
from airflow.decorators import dag, task

from operators.slack_operator import slack_error, slack_info

URL = "https://vdl-fullmakt.intern.nav.no/soda"  # run_job
APP = "https://vdl-fullmakt.intern.nav.no"


@dag(
    start_date=datetime(2023, 2, 14),
    schedule_interval=None,
    on_success_callback=slack_info,
    on_failure_callback=slack_error,
)
def run_fullmakt():
    @task()
    def start_job():
        res = requests.get(url=URL)
        data = res.json()
        if data["defaultDataSource"] == "fullmakter":
            slack_info(
                message="Fullmakter oppdatert", channel="#virksomhetsdatalaget-info"
            )
        else:
            slack_info(
                message=f"Fullmakter kjøring feilet. Sjekk {APP}",
                channel="#virksomhetsdatalaget-info",
            )

    start_job()


run_fullmakt()
