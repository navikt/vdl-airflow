from datetime import datetime

from airflow.decorators import dag, task

from airflow.models import Variable

from custom.operators.slack_operator import slack_error, slack_success, slack_info


@dag(
    start_date=datetime(2023, 7, 17),
    schedule_interval=None,
    on_success_callback=slack_success,
    on_failure_callback=slack_error,
)
def anaplan_ansattdata():
    wGuid = "8a868cda860a533a0186334e91805794"
    mGuid = "A07AB2A8DBA24E13B8A6E9EBCDB6235E"
    username = "virksomhetsdatalaget@nav.no"
    password = Variable.get("anaplan_password")

    @task
    def transfer(fileData: dict, query: str):
        from anaplan.singleChunkUpload import transfer_data
        import oracledb

        from io import StringIO
        import csv
        from sqlite3 import Cursor

        def get_data(query: str, cursor: Cursor):
            cursor.execute(query)
            column_names = map(lambda x: x[0], cursor.description)
            result = cursor.fetchall()
            print(f"Number of rows: {len(result)}")
            f = StringIO(newline="")
            writer = csv.writer(f)
            writer.writerow(column_names)
            writer.writerows(result)
            return f.getvalue()

        creds = Variable.get("dvh_password", deserialize_json=True)

        with oracledb.connect(**creds) as con:
            with con.cursor() as cursor:
                data = get_data(query, cursor)

        transfer_data(wGuid, mGuid, username, password, fileData, data)

    @task
    def update_data(importData: dict):
        from anaplan.import_data import import_data

        import_data(wGuid, mGuid, username, password, importData)

    upload = transfer.override(task_id="transfer_hr_data")(
        fileData={"id": "113000000040", "name": "anaplan_hrres_stillinger.csv"},
        query="""
                    select *
                    from DT_HR.ANAPLAN_HRRES_STILLINGER
                    """,
    )

    refresh_hierarchy_data = update_data.override(task_id="update_hierarchy_hr_data")(
        importData={
            "id": "112000000068",
            "name": "Test Ansatte Flat from anaplan_hrres_stillinger.csv",
        }
    )

    refresh_module_data = update_data.override(task_id="update_module_hr_data")(
        importData={
            "id": "112000000069",
            "name": "TEST 01.07 HR-Data from anaplan_hrres_stillinger.csv",
        }
    )

    (upload >> refresh_hierarchy_data >> refresh_module_data)


anaplan_ansattdata()
