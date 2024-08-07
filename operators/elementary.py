import os
from datetime import timedelta

import kubernetes.client as k8s
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

from custom.operators.slack_operator import slack_error


def elementary_operator(
    dag: DAG,
    task_id: str,
    commands: list[str],
    namespace: str = os.getenv("NAMESPACE"),
    retries: int = 3,
    extra_envs: dict = None,
    delete_on_finish: bool = True,
    startup_timeout_seconds: int = 360,
    retry_delay: timedelta = timedelta(seconds=120),
    nls_lang: str = "NORWEGIAN_NORWAY.AL32UTF8",
    image: str = "europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/vdl-airflow-elementary@sha256:5c42da6b6e2e581433fb124805de5e576904dc7881992db085a765d8fe16d620",
    allowlist: list = [],
    *args,
    **kwargs
):
    env_vars = {
        "TZ": os.environ["TZ"],
        "NLS_LANG": nls_lang,
        "KNADA_TEAM_SECRET": os.environ["KNADA_TEAM_SECRET"],
    }

    if extra_envs:
        env_vars = dict(env_vars, **extra_envs)

    return KubernetesPodOperator(
        dag=dag,
        cmds=["/bin/bash"],
        arguments=commands,
        on_failure_callback=slack_error,
        startup_timeout_seconds=startup_timeout_seconds,
        name=task_id,
        namespace=namespace,
        task_id=task_id,
        is_delete_operator_pod=delete_on_finish,
        image=image,
        image_pull_secrets=[k8s.V1LocalObjectReference("ghcr-secret")],
        env_vars=env_vars,
        service_account_name=os.getenv("TEAM"),
        annotations={
            "sidecar.istio.io/inject": "false",
            "allowlist": ",".join(allowlist),
        },
        retries=retries,
        retry_delay=retry_delay,
        **kwargs
    )
