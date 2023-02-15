from datetime import datetime

import requests
from airflow.decorators import dag, task

from operators.slack_operator import slack_error, slack_info

URL = "https://vdl-fullmakt.intern.nav.no/soda" #run_job
LOG = "https://vdl-fullmakt.intern.nav.no"


@dag(
    start_date=datetime(2023, 2, 14),
    schedule_interval=None
)
def run_fullmakt():
    @task()
    def start_job():
        try:
            res = requests.get(url=URL)
            data = res.json()
            if data.get("defaultDataSource") == "fullmakter":
                slack_info(
                    message="Fullmakter oppdatert", channel="#virksomhetsdatalaget-info"
                )
            else:
                slack_info(
                    message=f"Fullmakter kjøring feilet. Sjekk {LOG}.",
                    channel="#virksomhetsdatalaget-info",
            )
        except Exception as e:
            slack_info(
                message=f"Fullmakter kjøring feilet. Sjekk {LOG}. {e}",
                channel="#virksomhetsdatalaget-info",
            )

    start_job()

run_fullmakt()
