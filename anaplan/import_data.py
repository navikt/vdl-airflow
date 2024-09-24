# This script runs your selected import.

# This script uses Python 3 and assumes that you have the following modules
# installed: requests, json

import json

import requests
from auth import get_auth_response, get_header

base_url = "https://api.anaplan.com/2/0"


def import_data(wGuid: str, mGuid: str, username: str, password: str, importData: dict):

    auth_response = get_auth_response(username=username, password=password)
    import_header = get_header(auth_response=auth_response)

    importID = importData["id"]

    # Run an import post request
    postImport = requests.post(
        url=f"{base_url}/workspaces/{wGuid}/models/{mGuid}/imports/{importID}/tasks",
        headers=import_header,
        data=json.dumps({"localeName": "en_US"}),
    )

    if postImport.ok:
        print("Import request successful.")
    else:
        print(
            "There was an issue with your import request: "
            + str(postImport.status_code)
        )
        raise Exception("Noe gikk galt...")

    # Check status of import action
    taskID = postImport.json()["task"]["taskId"]
    getImportStatus = requests.get(
        url=f"{base_url}/workspaces/{wGuid}/models/{mGuid}/imports/{importID}/tasks/{taskID}",
        headers=import_header,
        data=json.dumps({"localeName": "en_US"}),
    )

    if getImportStatus.ok:
        print("Import status successful.")
    else:
        print(
            "There was an issue with the import status: "
            + str(getImportStatus.status_code)
        )
        raise Exception("Noe gikk galt...")

    # Get metadata for import action
    getImportMeta = requests.get(
        url=f"{base_url}/workspaces/{wGuid}/models/{mGuid}/imports/{importID}",
        headers=import_header,
        data=json.dumps({"localeName": "en_US"}),
    )

    # Check if dump file contains any errors
    # checkDump = requests.get(
    #    url=f"{base_url}/workspaces/{wGuid}/models/{mGuid}/imports/{importID}/tasks/{taskID}/dump",
    #    headers=import_header,
    #    data=json.dumps({"localeName": "en_US"}),
    # )

    # if checkDump.ok == False:
    #    print("No errors found")
