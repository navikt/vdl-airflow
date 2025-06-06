from typing import Optional

from airflow import DAG
from airflow.models import Variable

DBT_IMAGE = "ghcr.io/dbt-labs/dbt-snowflake:1.8.3@sha256:b95cc0481ec39cb48f09d63ae0f912033b10b32f3a93893a385262f4ba043f50"
SNOW_ALLOWLIST = [
    "wx23413.europe-west4.gcp.snowflakecomputing.com",
    "ocsp.snowflakecomputing.com",
    "ocsp.digicert.com:80",
    "o.pki.goog:80",
    "ocsp.pki.goo:80",
    "storage.googleapis.com",
]


def dbt_operator(
    task_id: str,
    repo: str,
    branch: str,
    command: str,
    target: Optional[str] = None,
    snowflake_user: Optional[str] = None,
    snowflake_password: Optional[str] = None,
):
    from dataverk_airflow import kubernetes_operator

    if target is None:
        target = Variable.get("dbt_target")
    snowflake_config = Variable.get("conn_snowflake", deserialize_json=True)
    return kubernetes_operator(
        dag=DAG,
        name=task_id,
        repo=repo,
        branch=branch,
        working_dir="dbt",
        cmds=["dbt deps", f"dbt { command }"],
        image=DBT_IMAGE,
        extra_envs={
            "SRV_USR": snowflake_user or snowflake_config["user"],
            "SRV_PWD": snowflake_password or snowflake_config["password"],
            "RUN_ID": "{{ run_id }}",
            "DBT_TARGET": target,
        },
        allowlist=[
            "hub.getdbt.com",
        ]
        + SNOW_ALLOWLIST,
        slack_channel=Variable.get("slack_error_channel"),
    )
