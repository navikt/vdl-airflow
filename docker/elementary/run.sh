#!/bin/bash
set -e

elementary () {
  edr $1 \
    --slack-token $SLACK_TOKEN \
    --slack-channel-name $2 \
    --target-path edr_target \
    --disable-samples true
}

if [ $1 = "report" ]; then
  elementary send-report $SLACK_INFO_CHANNEL
fi

if [ $1 = "alert" ]; then
  elementary monitor $SLACK_ALERT_CHANNEL
fi
