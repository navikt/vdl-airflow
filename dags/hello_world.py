from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.sensors.base import PokeReturnValue

# from custom.decorators import task
from kubernetes import client as k8s

from custom.decorators import CUSTOM_IMAGE
from custom.operators.slack_operator import (
    slack_error,
    slack_info,
    slack_success_old,
    test_slack,
)


@dag(
    start_date=datetime(2023, 2, 14),
    schedule_interval=None,
    on_success_callback=test_slack,
    on_failure_callback=slack_error,
    default_args={
        "executor_config": {
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"}),
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            image=CUSTOM_IMAGE,
                        )
                    ]
                ),
            )
        },
    },
)
def hello_world():
    @task.sensor(poke_interval=10, 
                 timeout=2 * 60 * 60)
    def send_slack_message(send: str): 
        if send=="True":
            slack_info(message="Hello, World!")
            return PokeReturnValue(is_done=True)
        if send=="False":
            return PokeReturnValue(is_done=True)
        else:
            raise AirflowFailException(
                "This task raised an Exception")
    send_message = send_slack_message.override(task_id="send_slack_message", outlets=[Dataset("hello_world")])("True")
    dont_send_message = send_slack_message.override(task_id="dont_send_slack_message")("False")

    send_message
    dont_send_message

hello_world()


@dag(
    start_date=datetime(2023, 2, 14),
    schedule_interval=None,
    schedule=[Dataset("hello_world")],
    on_success_callback=test_slack,
    on_failure_callback=slack_error,
    default_args={
        "executor_config": {
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"}),
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            image=CUSTOM_IMAGE,
                        )
                    ]
                ),
            )
        },
    },
)
def hello_world_2():
    @task()
    def send_slack_message():
        slack_info(message="Hello, Again!")

    slack_message = send_slack_message()

    slack_message


hello_world_2()
