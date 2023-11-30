from datetime import datetime

from airflow.decorators import dag, task

from airflow.models import Variable

from custom.operators.slack_operator import slack_error, slack_success, slack_info

from kubernetes import client as k8s


@dag(
    start_date=datetime(2023, 8, 25),
    schedule_interval=None,
    default_args={"on_failure_callback": slack_error},
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
                        "allowlist": ",".join(["slack.com", "api.anaplan.com"])
                    }
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
    )
    def clean(processData: dict):
        from anaplan.run_process import run_process

        run_process(wGuid, mGuid, username, password, processData)

    clean_module = clean.override(task_id="delete_hr_data_history")(
        processData={"id": "118000000012", "name": "P10 (X) Slett HR-data-historikk"}
    )

    @task(
        on_success_callback=slack_success,
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    annotations={
                        "allowlist": ",".join(
                            [
                                "slack.com",
                                "api.anaplan.com",
                                "dm09-scan.adeo.no:1521",
                            ]
                        )
                    }
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
    )
    def transfer(
        fileData: dict,
        query: str,
        import_hierarchy_data: dict,
        import_module_data: dict,
    ):
        from anaplan.singleChunkUpload import transfer_data
        from anaplan.get_data import get_data, transform_to_csv
        from anaplan.import_data import import_data
        import oracledb
        import time

        creds = Variable.get("dvh_password", deserialize_json=True)
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
