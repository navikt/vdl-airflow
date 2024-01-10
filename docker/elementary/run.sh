#!/bin/bash
set -e

secret_filename="secret.json"
access_token=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token | jq -r ".access_token")

curl "https://secretmanager.googleapis.com/v1/$KNADA_TEAM_SECRET/versions/latest:access" \
    --request "GET" \
    --header "authorization: Bearer $access_token" \
    --header "content-type: application/json" \
    | jq -r ".payload.data" | base64 --decode >> $secret_filename

export DBT_USR=$(cat $secret_filename | jq -r '.DBT_USR')
export DBT_PWD=$(cat $secret_filename | jq -r '.DBT_PWD')
export SLACK_TOKEN=$(cat $secret_filename | jq -r '.SLACK_TOKEN')
export SLACK_ALERT_CHANNEL=$(cat $secret_filename | jq -r '.SLACK_ALERT_CHANNEL')
export SLACK_INFO_CHANNEL=$(cat $secret_filename | jq -r '.SLACK_INFO_CHANNEL')

CHANNEL_ALERT=$SLACK_ALERT_CHANNEL
CHANNEL_INFO=$SLACK_INFO_CHANNEL

elementary () {
  edr $1 \
    --slack-token $SLACK_TOKEN \
    --slack-channel-name $2 \
    --target-path edr_target \
    --disable-samples true
}

if [ $1 = "report" ]; then
  elementary send-report $CHANNEL_INFO
fi

if [ $1 = "alert" ]; then
  elementary monitor $CHANNEL_ALERT
fi
