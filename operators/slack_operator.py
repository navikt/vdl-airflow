import os
from typing import Optional

from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.slack.operators.slack import SlackAPIPostOperator


def slack_info(
    context=None, message=None, channel: str = None, emoji=":information_source:"
):
    if channel is None:
        channel = Variable.get("slack_info_channel")
    if context is None:
        context = get_current_context()
    info_message = f"Airflow DAG: {context['dag'].dag_id} har kjørt ferdig."
    if message:
        info_message = f"{info_message}\n\n{message}"
    __slack_message(info_message, channel)


def slack_error(context=None, message=None, channel: str = None, emoji=":red_circle:"):
    if channel is None:
        channel = Variable.get("slack_error_channel")
    if context is None:
        context = get_current_context()
    error_message = f"En Airflow DAG feilet!\n\n- DAG: {context['dag'].dag_id}\n- Task: {context['task_instance'].task_id}"
    if message:
        error_message = f"{error_message}\n- Melding: {message}"
    __slack_message(error_message, channel)


def __slack_message(
    message: str,
    channel: str,
):
    SlackAPIPostOperator(
        task_id="slack-message",
        channel=channel,
        text=message,
        token=os.environ["SLACK_TOKEN"],
    ).execute()
