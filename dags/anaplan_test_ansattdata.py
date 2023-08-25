from datetime import datetime

from airflow.decorators import dag, task

from airflow.models import Variable

from custom.operators.slack_operator import slack_error, slack_success, slack_info


@dag(
    start_date=datetime(2023, 8, 4),
    schedule_interval=None,
    on_success_callback=slack_success,
    on_failure_callback=slack_error,
)
def anaplan_test_ansattdata():
    wGuid = "8a868cdc860a6af50186334b17be68b8"
    mGuid = "609BEDCBF89447DFACFE439152F903E1"
    username = "virksomhetsdatalaget@nav.no"
    password = Variable.get("anaplan_password")

    @task
    def clean(processData: dict):
        from anaplan.run_process import run_process

        run_process(wGuid, mGuid, username, password, processData)

    clean_module = clean.override(task_id="delete_hr_data_history")(
        processData={"id": "118000000011", "name": "P10 (X) Slette HR-data"}
    )

    @task
    def transfer(
        fileData: dict,
        query: str,
        import_hierarchy_data: dict,
        import_module_data: dict,
    ):
        from anaplan.singleChunkUpload import transfer_data
        from anaplan.get_data import get_data
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
        t_start = time.perf_counter()
        transfer_data(wGuid, mGuid, username, password, fileData, data)
        t_stop = time.perf_counter()
        print(f"transfer duration: {t_stop - t_start}")

        import_data(wGuid, mGuid, username, password, import_hierarchy_data)
        import_data(wGuid, mGuid, username, password, import_module_data)

    upload = transfer.override(task_id="transfer_hr_data")(
        fileData={"id": "113000000046", "name": "anaplan_hrres_stillinger_dvh.csv"},
        query="""
            select *
            from DT_HR.ANAPLAN_HRRES_STILLINGER
        """,
        import_hierarchy_data={
            "id": "112000000093",
            "name": "Test Ansatte Flat from anaplan_hrres_stillinger_dvh.csv",
        },
        import_module_data={
            "id": "112000000094",
            "name": "TEST 01.07 HR-Data from anaplan_hrres_stillinger_dvh.csv",
        },
    )

    (clean_module)
    (upload)


anaplan_test_ansattdata()
