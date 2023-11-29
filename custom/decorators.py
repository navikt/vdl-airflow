import functools
from typing import Callable
from airflow.decorators import task as airflow_task

from kubernetes import client as k8s


def task(allowlist: list[str] = [], **kwargs):
    def decorator(func):
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
                                image="europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/vdl-airflow@sha256:5edb4e907c93ee521f5f743c3b4346b1bae26721820a2f7e8dfbf464bf4c82ba",
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

    return decorator
