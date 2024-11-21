#!/bin/bash
set -e

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

source $SCRIPTPATH/.snowbird-venv/bin/activate

export PERMISSION_BOT_USER=$DBT_USR
export PERMISSION_BOT_ACCOUNT="wx23413.europe-west4.gcp"
export PERMISSION_BOT_WAREHOUSE="test_warehouse"
export PERMISSION_BOT_AUTHENTICATOR="externalbrowser"

export PERMISSION_BOT_DATABASE="faktura"
export PERMISSION_BOT_ROLE="SECURITYADMIN"

$SCRIPTPATH/.snowbird-venv/bin/snowbird run --path $SCRIPTPATH
