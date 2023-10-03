from datetime import datetime

from airflow.decorators import dag

from airflow.models import Variable

from custom.decorators import task

from custom.operators.slack_operator import slack_error, slack_success, slack_info


@dag(
    start_date=datetime(2023, 8, 16),
    schedule_interval="daily",
    catchup=False
    on_success_callback=slack_success,
    on_failure_callback=slack_error,
)
def anaplan_datahub_regnskapsdata():
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

    upload = transfer.override(task_id="transfer_regnskapsdata")(
        fileData={
            "id": "113000000035",
            "name": "agg_hovedbok_posteringer_all_mnd_snowflake.csv",
        },
        query="""
            select
                md5(
                    periode_navn||
                    statsregnskapskonti_segment_kode||
                    artskonti_segment_kode||
                    kostnadssteder_segment_kode||
                    produkter_segment_kode||
                    oppgaver_segment_kode||
                    felles_segment_kode
                ) as pk,
                periode_navn,
                statsregnskapskonti_segment_kode,
                artskonti_segment_kode,
                kostnadssteder_segment_kode,
                produkter_segment_kode,
                oppgaver_segment_kode,
                felles_segment_kode,
                sum(sum_netto_nok) as sum_netto_nok
            from reporting.microstrategy.agg_hovedbok_posteringer_all_mnd
            where
                ER_BUDSJETT_POSTERING = 0 and
                hovedbok_id = '3022' and
                endswith(periode_navn, '23') and
                length(artskonti_segment_kode) = 12 and
                endswith(statsregnskapskonti_segment_kode, '000000')
            group by 1,2,3,4,5,6,7,8
        """,
        import_hierarchy_data={
            "id": "112000000053",
            "name": "Regnskap Flat from agg_hovedbok_posteringer_all_mnd_snowflak",
        },
        import_module_data={
            "id": "112000000054",
            "name": "Regnskap from agg_hovedbok_posteringer_all_mnd_snowflake.csv",
        },
    )

    (upload)


anaplan_datahub_regnskapsdata()
