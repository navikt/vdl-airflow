import functools
from typing import Callable
from airflow.decorators import task as airflow_task

from kubernetes import client as k8s


def task(func: Callable, allowlist: list[str] = [], **kwargs):
    @airflow_task(
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    annotations={"allowlist": ",".join(allowlist)}
                ),
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            image="europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/vdl-airflow@sha256:5a2d6ad5da2e264fdfe1be4182fb7d7ff34ac2b0f1a731899dff63e93d2c0541",
                        )
                    ]
                ),
            )
        },
        **kwargs
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        value = func(*args, **kwargs)
        return value

    return wrapper
