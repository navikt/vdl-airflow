import os
from datetime import timedelta
from typing import Optional

import kubernetes.client as k8s
from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

from custom.operators.slack_operator import slack_error

SNOW_ALLOWLIST = [
    "wx23413.europe-west4.gcp.snowflakecomputing.com",
    "ocsp.snowflakecomputing.com",
    "ocsp.digicert.com:80",
    "o.pki.goog:80",
    "ocsp.pki.goo:80",
    "storage.googleapis.com",
]


def elementary_operator(
    dag: DAG,
    task_id: str,
    commands: list[str],
    database: str,
    schema: str,
    snowflake_role: str,
    snowflake_warehouse: str,
    dbt_docs_project_name: Optional[str] = None,
    namespace: str = os.getenv("NAMESPACE"),
    retries: int = 3,
    extra_envs: dict = None,
    delete_on_finish: bool = True,
    startup_timeout_seconds: int = 360,
    retry_delay: timedelta = timedelta(seconds=120),
    nls_lang: str = "NORWEGIAN_NORWAY.AL32UTF8",
    image: str = "europe-north1-docker.pkg.dev/nais-management-233d/virksomhetsdatalaget/vdl-airflow-elementary@sha256:2f8187434db61ead6a32478ca82210733589c277dc8a4c744ccd0afe0c4f6610",
    allowlist: list = [],
    dbt_docs_uri = f"dbt.intern.{Variable.get('nav_subdomain')}",
    dbt_docs_for_slack_uri = f"dbt.ansatt.{Variable.get('nav_subdomain')}",
    *args,
    **kwargs,
):
    
    
    env_vars = {
        "TZ": os.environ["TZ"],
        "NLS_LANG": nls_lang,
        "KNADA_TEAM_SECRET": os.environ["KNADA_TEAM_SECRET"],
        "DBT_USR": Variable.get("srv_snowflake_user"),
        "DBT_PWD": Variable.get("srv_snowflake_password"),
        "DBT_DOCS_URL": f"https://{dbt_docs_uri}",
        "DBT_DOCS_FOR_SLACK_URL": f"https://{dbt_docs_for_slack_uri}",
        "SLACK_TOKEN": Variable.get("slack_token"),
        "SLACK_ALERT_CHANNEL": Variable.get("slack_error_channel"),
        "SLACK_INFO_CHANNEL": Variable.get("slack_info_channel"),
        "DB": database,
        "DB_ROLE": snowflake_role,
        "DB_WH": snowflake_warehouse,
        "DBT_PROSJEKT": dbt_docs_project_name,
    }

    allowlist = (
        [
            "slack.com",
            "files.slack.com",
            dbt_docs_uri,
        ]
        + SNOW_ALLOWLIST
        + allowlist
    )

    if extra_envs:
        env_vars = dict(env_vars, **extra_envs)

    return KubernetesPodOperator(
        dag=dag,
        cmds=["/bin/bash", "./run.sh"],
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
        **kwargs,
    )
