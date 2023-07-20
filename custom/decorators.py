import functools
from typing import Callable
from airflow.decorators import task as airflow_task

from kubernetes import client as k8s


def task(func: Callable, **kwargs):
    @airflow_task(
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            image="ghcr.io/navikt/vdl-airflow:2739f712d781142f78e173f76bb0be31d17b94df",
                        )
                    ]
                )
            )
        },
        **kwargs
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        value = func(*args, **kwargs)
        return value

    return wrapper
