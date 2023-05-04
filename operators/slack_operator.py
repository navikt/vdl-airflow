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
    if message is None:
        message = f"Airflow DAG: {context['dag'].dag_id} har kjørt ferdig."
    __slack_message(context, message, channel, emoji)


def slack_error(context=None, message=None, channel: str = None, emoji=":red_circle:"):
    if channel is None:
        channel = Variable.get("slack_error_channel")
    if message is None:
        message = f"En Airflow DAG feilet!\n\n- DAG: {context['dag'].dag_id}\n- Task: {context['task_instance'].task_id}"
    __slack_message(context, message, channel, emoji)


def __slack_message(
    context: str,
    message: str,
    channel: str,
    emoji: str,
    attachments: Optional[list] = None,
):
    if context is None:
        context = get_current_context()
    SlackAPIPostOperator(
        task_id="slack-message",
        channel=channel,
        text=message,
        token=os.environ["SLACK_TOKEN"],
    ).execute()
