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


def module_data():
    # Insert your workspace Guid
    wGuid = "8a868cda860a533a0186334e91805794"
    # Insert your model Guid
    mGuid = "A07AB2A8DBA24E13B8A6E9EBCDB6235E"
    # Insert the Anaplan account email being used
    username = "virksomhetsdatalaget@nav.no"

    # Replace with your import metadata
    importData = {
        "id": "112000000051",
        "name": "TEST 01.02 Test Kontostruktur 2 from dim_artskonti.csv",
        "importDataSourceId": "113000000033",
        "importType": "MODULE_DATA",
    }

    # If using basic auth, insert your password. Otherwise, remove this line.
    password = Variable.get("anaplan_password")

    # Uncomment your authentication method (cert or basic). Remove the other.

    user = "Basic " + str(
        base64.b64encode((f"{username}:{password}").encode("utf-8")).decode("utf-8")
    )

    url = (
        f"https://api.anaplan.com/1/3/workspaces/{wGuid}/models/{mGuid}/"
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
