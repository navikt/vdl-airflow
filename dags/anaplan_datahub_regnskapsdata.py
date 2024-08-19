from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from kubernetes import client as k8s

from custom.decorators import CUSTOM_IMAGE
from custom.operators.slack_operator import slack_error, slack_info, slack_success_old


@dag(
    start_date=datetime(2023, 8, 16),
    schedule_interval="0 4 * * *",
    catchup=False,
    default_args={"on_failure_callback": slack_error, "retries": 3},
)
def anaplan_datahub_regnskapsdata():
    wGuid = Variable.get("anaplan_workspace_id")
    mGuid = Variable.get("anaplan_model_id")
    username = Variable.get("anaplan_username")
    password = Variable.get("anaplan_password")

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
                                "wx23413.europe-west4.gcp.snowflakecomputing.com",
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
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        from anaplan.get_data import get_data, transform_to_csv
        from anaplan.import_data import import_data
        from anaplan.singleChunkUpload import transfer_data

        with SnowflakeHook().get_cursor() as cursor:
            data, column_names = get_data(query, cursor)

        csv_file = transform_to_csv(data=data, column_names=column_names)

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
              k.periode_navn||
              ds.statsregnskapskonti_segment_kode_niva_2||'000000'||
              k.artskonti_segment_kode||
              k.kostnadssteder_segment_kode||
              k.produkter_segment_kode||
              k.oppgaver_segment_kode||
              k.felles_segment_kode
          ) as pk,
          k.periode_navn,
          -- Lagt til 6 nuller, pga. bakoverkompatibilitet
          ds.statsregnskapskonti_segment_kode_niva_2||'000000' as statsregnskapskonti_segment_kode,
          k.artskonti_segment_kode,
          k.kostnadssteder_segment_kode,
          k.produkter_segment_kode,
          k.oppgaver_segment_kode,
          k.felles_segment_kode,
          sum(netto_nok) as sum_netto_nok
              from regnskap.marts.fak_kontant_hovedbok_posteringer_v0 k
          join regnskap.marts.dim_statsregnskapskonti ds on 1=1
            and ds.pk_dim_statsregnskapskonti = k.fk_dim_statsregnskapskonti
          where 1=1
            and k.er_budsjett_postering = 0
            and (
              endswith(k.periode_navn, '23') or
              endswith(k.periode_navn, '24')
            )
          group by all
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

    upload


anaplan_datahub_regnskapsdata()
