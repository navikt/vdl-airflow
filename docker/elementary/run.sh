#!/bin/bash
set -e

elementary () {
  edr $1 \
    --slack-token $SLACK_TOKEN \
    --slack-channel-name $2 \
    --target-path edr_target \
    --disable-samples true
}

elementary2 () {
  edr $1 \
    --slack-token $SLACK_TOKEN \
    --slack-channel-name $2 \
    --target-path edr_target \
    --disable-samples true \
    --disable html_attachment
}

if [ $1 = "report" ]; then
  elementary send-report $SLACK_INFO_CHANNEL
  echo "Report sent to $SLACK_INFO_CHANNEL"
  exit 0
fi

if [ $1 = "alert" ]; then
  elementary monitor $SLACK_ALERT_CHANNEL
  echo "Alert sent to $SLACK_ALERT_CHANNEL"
  exit 0
fi

if [ $1 = "dbt_docs" ]; then
  elementary2 send-report $SLACK_INFO_CHANNEL
  curl -X PUT \
    -F index.html=@edr_target/elementary_report.html \
    $DBT_DOCS_URL/docs/virksomhetsdatalaget/$DBT_PROSJEKT
  curl -d "text=DBT docs updated at $DBT_DOCS_FOR_SLACK_URL/docs/virksomhetsdatalaget/$DBT_PROSJEKT" \
    -d "channel=$SLACK_INFO_CHANNEL" \
    -H "Authorization: Bearer $SLACK_TOKEN" \
    -X POST https://slack.com/api/chat.postMessage
  echo "DBT docs updated"
  exit 0
fi

echo "Unknown command"
exit 1
