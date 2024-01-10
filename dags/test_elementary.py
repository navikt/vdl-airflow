from datetime import datetime
from airflow import DAG

from airflow.decorators import dag, task

from custom.operators.slack_operator import slack_error, slack_success, slack_info
#from custom.decorators import task
from kubernetes import client as k8s

from operators.elementary import elementary_operator


with DAG(
    dag_id="test_elementary",
    start_date=datetime(2024, 1, 9),
    schedule=None,
    default_args={
        "executor_config": {
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"})
            )
        },
    }
) as dag:
    elementary_report = elementary_operator(
        dag=dag,
        task_id="elementary_report",
        commands=["./run.sh", "report"],
        allowlist=["slack.com", "files.slack.com, wx23413.europe-west4.gcp.snowflakecomputing.com"]
    )

    elementary_alert = elementary_operator(
        dag=dag,
        task_id="elementary_alert",
        commands=["./run.sh", "report"],
        allowlist=["slack.com", "files.slack.com, wx23413.europe-west4.gcp.snowflakecomputing.com"]
    )

    elementary_report
    elementary_alert
