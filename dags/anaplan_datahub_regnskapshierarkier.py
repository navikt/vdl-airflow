from datetime import datetime

from airflow.decorators import dag

from airflow.models import Variable

from custom.decorators import task

from custom.operators.slack_operator import slack_error, slack_success, slack_info


@dag(
    start_date=datetime(2023, 8, 2),
    schedule_interval=None,
    on_success_callback=slack_success,
    on_failure_callback=slack_error,
)
def anaplan_datahub_regnskaphierarkier():
    wGuid = Variable.get("anaplan_workspace_id")
    mGuid = Variable.get("anaplan_model_id")
    username = Variable.get("anaplan_username")
    password = Variable.get("anaplan_password")

    @task
    def transfer(
        fileData: dict,
        query: str,
        import_hierarchy_data: dict,
        import_module_data: dict,
    ):
        from anaplan.singleChunkUpload import transfer_data
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from anaplan.get_data import get_data, transform_to_csv
        from anaplan.import_data import import_data

        with SnowflakeHook().get_cursor() as cursor:
            data = get_data(query, cursor)

        csv_file = transform_to_csv(data[0], data[1])

        transfer_data(wGuid, mGuid, username, password, fileData, csv_file)
        import_data(wGuid, mGuid, username, password, import_hierarchy_data)
        import_data(wGuid, mGuid, username, password, import_module_data)

    upload_artskonti = transfer.override(task_id="transfer_artskonti")(
        fileData={"id": "113000000029", "name": "dim_artskonti_snowflake.csv"},
        query="""
            select *
            from reporting.microstrategy.dim_artskonti
            where
                endswith(artskonti_segment_kode, '0000000') and
                er_budsjetterbar=1
        """,
        import_hierarchy_data={
            "id": "112000000041",
            "name": "Artskonti Flat from dim_artskonti_snowflake.csv",
        },
        import_module_data={
            "id" : "112000000066",
            "name" : "SYS 02.01 Kontostruktur from dim_artskonti_snowflake.csv",
        },
    )

    upload_kostnadssteder = transfer.override(task_id="transfer_kostnadssteder")(
        fileData={"id": "113000000030", "name": "dim_kostnadssteder_snowflake.csv"},
        query="""
            select *
            from reporting.microstrategy.dim_kostnadssteder
            where
                er_budsjetterbar = 1
        """,
        import_hierarchy_data={
            "id": "112000000043",
            "name": "Ksted Flat from dim_kostnadssteder_snowflake.csv",
        },
        import_module_data={
            "id" : "112000000067",
            "name" : "SYS 03.01 Organisasjonsstru from dim_kostnadssteder_snowflak",
        },
    )

    upload_produkter = transfer.override(task_id="transfer_produkter")(
        fileData={"id": "113000000031", "name": "dim_produkter_snowflake.csv"},
        query="""
            select *
            from reporting.microstrategy.dim_produkter
            where
                er_budsjetterbar = 1
        """,
        import_hierarchy_data={
            "id": "112000000045",
            "name": "Produkt Flat from dim_produkter_snowflake.csv",
        },
        import_module_data={
            "id": "112000000046",
            "name": "Produkt from dim_produkter_snowflake.csv",
        },
    )

    upload_oppgaver = transfer.override(task_id="transfer_oppgaver")(
        fileData={"id": "113000000032", "name": "dim_oppgaver_snowflake.csv"},
        query="""
            with

            statskonti as (
                select distinct
                    oppgaver_segment_kode
                    ,statsregnskapskonti_segment_kode
                from reporting.microstrategy.fak_hovedbok_posteringer
                where er_budsjett = 1
            )

            ,oppgaver as (
                select * from reporting.microstrategy.dim_oppgaver where er_budsjetterbar = 1
            )

            select
                oppgaver.*
                ,statskonti.statsregnskapskonti_segment_kode
                ,case
                    when statskonti.statsregnskapskonti_segment_kode is null then
                        oppgaver.oppgaver_segment_kode else
                        concat(oppgaver.oppgaver_segment_kode,'_',statskonti.statsregnskapskonti_segment_kode)
                end as pk_oppgaver_statskonti
            from oppgaver
            left join statskonti on
                statskonti.oppgaver_segment_kode = oppgaver.oppgaver_segment_kode
        """,
        import_hierarchy_data={
            "id": "112000000047",
            "name": "Oppgave Flat from dim_oppgaver_snowflake.csv",
        },
        import_module_data={
            "id": "112000000048",
            "name": "Oppgave from dim_oppgaver_snowflake.csv",
        },
    )

    upload_felles = transfer.override(task_id="transfer_felles")(
        fileData={"id": "113000000033", "name": "dim_felles_snowflake.csv"},
        query="""
            select *
            from reporting.microstrategy.dim_felles
            where
                er_budsjetterbar = 1
        """,
        import_hierarchy_data={
            "id": "112000000049",
            "name": "Felles Flat from dim_felles_snowflake.csv",
        },
        import_module_data={
            "id": "112000000050",
            "name": "Felles from dim_felles_snowflake.csv",
        },
    )

    upload_statsregnskapskonti = transfer.override(
        task_id="transfer_statsregnskapskonti"
    )(
        fileData={
            "id": "113000000034",
            "name": "dim_statsregnskapskonti_snowflake.csv",
        },
        query="""
            select *
            from reporting.microstrategy.dim_statsregnskapskonti
            where
                endswith(statsregnskapskonti_segment_kode, '000000') and
                er_budsjetterbar=1
        """,
        import_hierarchy_data={
            "id": "112000000051",
            "name": "Statskonto Flat from dim_statsregnskapskonti_snowflake.csv",
        },
        import_module_data={
            "id": "112000000052",
            "name": "Statskonti from dim_statsregnskapskonti_snowflake.csv",
        },
    )

    (upload_artskonti)

    (upload_kostnadssteder)

    (upload_produkter)

    (upload_oppgaver)

    (upload_felles)

    (upload_statsregnskapskonti)


anaplan_datahub_regnskaphierarkier()
