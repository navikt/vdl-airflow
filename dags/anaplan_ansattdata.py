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
    def transfer(
        fileData: dict,
        query: str,
        hierarchy_import_data: dict,
        module_import_data: dict,
    ):
        from anaplan.import_data import import_data
        from anaplan.singleChunkUpload import transfer_data
        from anaplan.get_data import get_data
        import oracledb

        creds = Variable.get("dvh_password", deserialize_json=True)
        with oracledb.connect(**creds) as con:
            with con.cursor() as cursor:
                data = get_data(query, cursor)
        transfer_data(wGuid, mGuid, username, password, fileData, data)
        import_data(
            wGuid=wGuid,
            mGuid=mGuid,
            username=username,
            password=password,
            importData=hierarchy_import_data,
        )
        import_data(
            wGuid=wGuid,
            mGuid=mGuid,
            username=username,
            password=password,
            importData=module_import_data,
        )

    upload = transfer.override(task_id="transfer_hr_data")(
        fileData={"id": "113000000040", "name": "anaplan_hrres_stillinger.csv"},
        query="""
            select *
            from dt_hr.anaplan_hrres_stillinger
        """,
        hierarchy_import_data={
            "id": "112000000072",
            "name": "Test Ansatte Fla from anaplan_hrres_stillinger.csv",
        },
        module_import_data={
            "id": "112000000073",
            "name": "TEST 01.07 HR-Data - Copy from anaplan_hrres_stillinger.csv",
        },
    )

    upload


anaplan_ansattdata()
