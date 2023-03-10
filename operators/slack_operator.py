import os
from typing import Optional

from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


def slack_info(
    context=None, message=None, channel: str = None, emoji=":information_source:"
):
    if channel is None:
        channel = Variable.get("slack_info_channel")
    if message is None:
        message = f"Airflow DAG: {context['dag'].dag_id} har kjørt ferdig."
    __slack_message(context, message, channel, emoji)


def slack_error(context=None, channel: str = None, emoji=":red_circle:"):
    if channel is None:
        channel = Variable.get("slack_error_channel")
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
    SlackWebhookOperator(
        http_conn_id=None,
        task_id="slack-message",
        webhook_token=os.environ["SLACK_TOKEN"],
        message=message,
        channel=channel,
        link_names=True,
        icon_emoji=emoji,
        attachments=attachments,
    ).execute(context)
