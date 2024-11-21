#!/bin/bash
set -e

url=https://$HOST/docs/virksomhetsdatalaget/$DBT_PROSJEKT

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
    --disable html_attachment \
    --s3-endpoint-url $url
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
    $url
  curl -d "text=DBT docs updated at $url" \
    -d "channel=$SLACK_INFO_CHANNEL" \
    -H "Authorization: Bearer $SLACK_TOKEN" \
    -X POST https://slack.com/api/chat.postMessage
  echo "DBT docs updated at $url"
  exit 0
fi

echo "Unknown command"
exit 1
