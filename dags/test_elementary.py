from datetime import datetime

from airflow.decorators import dag, task

from custom.operators.slack_operator import slack_error, slack_success, slack_info
#from custom.decorators import task
from kubernetes import client as k8s

from operators.elementary import elementary_operator


@dag(
    start_date=datetime(2023, 2, 14),
    schedule_interval=None,
    on_success_callback=slack_success,
    on_failure_callback=slack_error,
    default_args={
        "executor_config": {
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"})
            )
        },
    },
)
def test_elementary():
    elementary_report = elementary_operator(
        dag=test_elementary,
        task_id="elementary_report",
        commands=["report"],
        allowlist=["slack.com", "files.slack.com"]
    )

    elementary_report

test_elementary()
