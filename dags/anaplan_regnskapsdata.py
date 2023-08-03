from datetime import datetime

from airflow.decorators import dag

from airflow.models import Variable

from custom.decorators import task

from custom.operators.slack_operator import slack_error, slack_success, slack_info


@dag(
    start_date=datetime(2023, 8, 3),
    schedule_interval=None,
    on_success_callback=slack_success,
    on_failure_callback=slack_error,
)
def anaplan_regnskapsdata():
    wGuid = "8a868cda860a533a0186334e91805794"
    mGuid = "A07AB2A8DBA24E13B8A6E9EBCDB6235E"
    username = "virksomhetsdatalaget@nav.no"
    password = Variable.get("anaplan_password")

    @task
    def transfer(fileData: dict, query: str):
        from anaplan.singleChunkUpload import transfer_data
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from anaplan.get_data import get_data

        with SnowflakeHook().get_cursor() as cursor:
            data = get_data(query, cursor)

        transfer_data(wGuid, mGuid, username, password, fileData, data)

    @task
    def update_data(importData: dict):
        from anaplan.import_data import import_data

        import_data(wGuid, mGuid, username, password, importData)

    upload = transfer.override(task_id="transfer_regnskapsdata")(
        fileData={
            "id": "113000000044",
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
    endswith(artskonti_segment_kode, '0000000') and
    endswith(statsregnskapskonti_segment_kode, '000000')
group by 1,2,3,4,5,6,7,8
                """,
    )

    refresh_hierarchy_data = update_data.override(
        task_id="update_hierarchy_regnskapsdata"
    )(
        importData={
            "id": "112000000086",
            "name": "Test Regnskap Flat from agg_hovedbok_posteringer_all_mnd_sno",
        }
    )

    refresh_module_data = update_data.override(task_id="update_module_regnskapsdata")(
        importData={
            "id": "112000000087",
            "name": "TEST 01.08 Regnskap from agg_hovedbok_posteringer_all_mnd_sn",
        }
    )

    (upload >> refresh_hierarchy_data >> refresh_module_data)


anaplan_regnskapsdata()
