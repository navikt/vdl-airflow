from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from kubernetes import client as k8s

from custom.decorators import CUSTOM_IMAGE
from custom.operators.slack_operator import slack_error, slack_info, slack_success_old


@dag(
    start_date=datetime(2023, 8, 25),
    schedule_interval="0 5 * * *",
    catchup=False,
    default_args={"on_failure_callback": slack_error, "retries": 3},
)
def anaplan_datahub_ansattdata():
    wGuid = Variable.get("anaplan_workspace_id")
    mGuid = Variable.get("anaplan_model_id")
    username = Variable.get("anaplan_username")
    password = Variable.get("anaplan_password")

    @task(
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    annotations={
                        "allowlist": ",".join(
                            ["slack.com", "api.anaplan.com", "auth.anaplan.com"]
                        )
                    }
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
    def clean(processData: dict):
        from anaplan.run_process import run_process

        run_process(wGuid, mGuid, username, password, processData)

    clean_module = clean.override(task_id="delete_hr_data_history")(
        processData={"id": "118000000012", "name": "P10 (X) Slett HR-data-historikk"}
    )

    @task(
        on_success_callback=slack_success_old,
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    annotations={
                        "allowlist": ",".join(
                            [
                                "slack.com",
                                "api.anaplan.com",
                                "auth.anaplan.com",
                                "dmv09-scan.adeo.no:1521",
                            ]
                        )
                    }
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
    def transfer(
        fileData: dict,
        query: str,
        import_hierarchy_data: dict,
        import_module_data: dict,
    ):
        import time

        import oracledb

        from anaplan.get_data import get_data, transform_to_csv
        from anaplan.import_data import import_data
        from anaplan.singleChunkUpload import transfer_data

        creds = {
            "user": Variable.get("dvh_user"),
            "password": Variable.get("dvh_password"),
            "dsn": Variable.get("dvh_dsn"),
        }
        ora_start = time.perf_counter()
        with oracledb.connect(**creds) as con:
            with con.cursor() as cursor:
                data = get_data(query, cursor)
        ora_stop = time.perf_counter()
        print(f"oracle duration: {ora_stop - ora_start}")
        ora_start = time.perf_counter()
        csv_file = transform_to_csv(data[0], data[1])
        ora_stop = time.perf_counter()
        print(f"transform to csv duration: {ora_stop - ora_start}")
        t_start = time.perf_counter()
        transfer_data(wGuid, mGuid, username, password, fileData, csv_file)
        t_stop = time.perf_counter()
        print(f"transfer duration: {t_stop - t_start}")

        import_data(wGuid, mGuid, username, password, import_hierarchy_data)
        import_data(wGuid, mGuid, username, password, import_module_data)

    upload = transfer.override(task_id="transfer_hr_data")(
        fileData={"id": "113000000037", "name": "anaplan_hrres_stillinger_dvh.csv"},
        query="""
            select *
            from DT_HR.ANAPLAN_HRRES_STILLINGER
        """,
        import_hierarchy_data={
            "id": "112000000059",
            "name": "Ansatte Flat from anaplan_hrres_stillinger_dvh.csv",
        },
        import_module_data={
            "id": "112000000061",
            "name": "HR-Data from anaplan_hrres_stillinger_dvh.csv",
        },
    )

    clean_module >> upload


anaplan_datahub_ansattdata()
