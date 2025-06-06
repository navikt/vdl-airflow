#!/bin/bash
set -e

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

source $SCRIPTPATH/.snowbird-venv/bin/activate

$SCRIPTPATH/.snowbird-venv/bin/snowbird $@
