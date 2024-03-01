import functools
from typing import Callable

from airflow.decorators import task as airflow_task
from kubernetes import client as k8s

CUSTOM_IMAGE="europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/vdl-airflow@sha256:85e5787aaaf694379fb031954cb53a04a80fcc08934991fcb90fae65102d5fe3"

def task(allowlist: list[str] = ["slack.com"]):
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
                            image=CUSTOM_IMAGE,
                        )
                    ]
                ),
            )
        },
    )
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            value = func(*args, **kwargs)
            return value

        return wrapper

    return decorator
