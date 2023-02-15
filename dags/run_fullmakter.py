from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from kubernetes import client as k8s

URL = "https://vdl-fullmakt.intern.nav.no/soda"  # run_job
LOG = "https://vdl-fullmakt.intern.nav.no"


def run_job():
    import requests

    from operators.slack_operator import slack_info

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


with DAG("run_fullmakt", start_date=days_ago(1), schedule_interval=None) as dag:
    run_task = PythonOperator(
        task_id="run_fullmakt_task",
        python_callable=run_job,
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": URL})
            )
        },
        dag=dag,
    )

    run_task
