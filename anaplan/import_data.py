# This script runs your selected import. Run 'importStatus.py' to retrieve
# the task metadata for the import task.

# This script assumes you know your workspaceGuid, modelGuid, and import
# metadata.
# If you do not have this information, please run 'getWorkspaces.py',
# 'getModels.py', and 'getImports.py' and retrieve this information from the
# resulting json files.

# If you are using certificate authentication, this script assumes you have
# converted your Anaplan certificate to PEM format, and that you know the
# Anaplan account email associated with that certificate.

# This script uses Python 3 and assumes that you have the following modules
# installed: requests, base64, json

import requests
import base64
import sys
import json
from airflow.models import Variable


def import_data(wGuid:str, mGuid:str, username:str, password:str, importData:dict):

    user = "Basic " + str(
        base64.b64encode((f"{username}:{password}").encode("utf-8")).decode("utf-8")
    )

    url = (
        f"https://api.anaplan.com/2/0/workspaces/{wGuid}/models/{mGuid}/"
        + f'imports/{importData["id"]}/tasks'
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
