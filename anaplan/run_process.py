# This script runs your selected process. Run 'processStatus.py' to retrieve
# the task metadata for the process task.

# This script assumes you know your workspaceGuid, modelGuid, and process
# metadata.
# If you do not have this information, please run 'getWorkspaces.py',
# 'getModels.py', and 'getProcesses.py' and retrieve this information from the
# resulting json files.

# If you are using certificate authentication, this script assumes you have
# converted your Anaplan certificate to PEM format, and that you know the
# Anaplan account email associated with that certificate.

# This script uses Python 3 and assumes that you have the following modules
# installed: requests, base64, json

import base64
import json
import sys

import requests
from airflow.models import Variable


def run_process(
    wGuid: str, mGuid: str, username: str, password: str, processData: dict
):
    user = "Basic " + str(
        base64.b64encode((f"{username}:{password}").encode("utf-8")).decode("utf-8")
    )

    url = (
        f"https://api.anaplan.com/1/3/workspaces/{wGuid}/models/{mGuid}/"
        + f'processes/{processData["id"]}/tasks'
    )

    postHeaders = {"Authorization": user, "Content-Type": "application/json"}

    # Runs an import request, and returns task metadata to 'postImport.json'
    print(url)
    postImport = requests.post(
        url, headers=postHeaders, data=json.dumps({"localeName": "en_US"})
    )

    print(postImport.status_code)
    print(postImport.text.encode("utf-8"))
    if postImport.status_code != 200:
        raise Exception("Noe gikk galt")
