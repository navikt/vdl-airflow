from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable
from kubernetes import client as k8s

from custom.decorators import CUSTOM_IMAGE
from custom.operators.slack_operator import slack_error, slack_info, slack_success_old


@dag(
    start_date=datetime(2023, 8, 2),
    schedule_interval=None,
    schedule=[Dataset("regnskap_dataset")],
    catchup=False,
    default_args={"on_failure_callback": slack_error, "retries": 3},
)
def anaplan_datahub_regnskaphierarkier():
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
                            [
                                "slack.com",
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
    def send_slack_notification():
        slack_success_old()

    @task(
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    annotations={
                        "allowlist": ",".join(
                            [
                                "slack.com",
                                "api.anaplan.com",
                                "auth.anaplan.com",
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
        """,
        import_hierarchy_data={
            "id": "112000000041",
            "name": "Artskonti Flat from dim_artskonti_snowflake.csv",
        },
        import_module_data={
            "id": "112000000066",
            "name": "SYS 02.01 Kontostruktur from dim_artskonti_snowflake.csv",
        },
    )

    upload_kostnadssteder = transfer.override(task_id="transfer_kostnadssteder")(
        fileData={"id": "113000000030", "name": "dim_kostnadssteder_snowflake.csv"},
        query="""
            select *
            from reporting.microstrategy.dim_kostnadssteder
        """,
        import_hierarchy_data={
            "id": "112000000043",
            "name": "Ksted Flat from dim_kostnadssteder_snowflake.csv",
        },
        import_module_data={
            "id": "112000000067",
            "name": "SYS 03.01 Organisasjonsstru from dim_kostnadssteder_snowflak",
        },
    )

    upload_produkter = transfer.override(task_id="transfer_produkter")(
        fileData={"id": "113000000031", "name": "dim_produkter_snowflake.csv"},
        query="""
            select *
            from reporting.microstrategy.dim_produkter
        """,
        import_hierarchy_data={
            "id": "112000000045",
            "name": "Produkt Flat from dim_produkter_snowflake.csv",
        },
        import_module_data={
            "id": "112000000068",
            "name": "SYS 04.01 Produkthierarki from dim_produkter_snowflake.csv",
        },
    )

    upload_oppgaver = transfer.override(task_id="transfer_oppgaver")(
        fileData={"id": "113000000032", "name": "dim_oppgaver_snowflake.csv"},
        query="""
            select *
            from reporting.microstrategy.dim_oppgaver
        """,
        import_hierarchy_data={
            "id": "112000000047",
            "name": "Oppgave Flat from dim_oppgaver_snowflake.csv",
        },
        import_module_data={
            "id": "112000000069",
            "name": "SYS 05.01 Oppgave from dim_oppgaver_snowflake.csv",
        },
    )

    upload_felles = transfer.override(task_id="transfer_felles")(
        fileData={"id": "113000000033", "name": "dim_felles_snowflake.csv"},
        query="""
            select *
            from reporting.microstrategy.dim_felles
        """,
        import_hierarchy_data={
            "id": "112000000049",
            "name": "Felles Flat from dim_felles_snowflake.csv",
        },
        import_module_data={
            "id": "112000000070",
            "name": "SYS 05.02 Felles from dim_felles_snowflake.csv",
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
        """,
        import_hierarchy_data={
            "id": "112000000051",
            "name": "Statskonto Flat from dim_statsregnskapskonti_snowflake.csv",
        },
        import_module_data={
            "id": "112000000071",
            "name": "SYS 06.01 Statskontohierark from dim_statsregnskapskonti_sno",
        },
    )

    slack_notification = send_slack_notification()

    upload_artskonti >> slack_notification

    upload_kostnadssteder >> slack_notification

    upload_produkter >> slack_notification

    upload_oppgaver >> slack_notification

    upload_felles >> slack_notification

    upload_statsregnskapskonti >> slack_notification


anaplan_datahub_regnskaphierarkier()
