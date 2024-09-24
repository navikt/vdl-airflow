# This script uploads a file in a single chunk.
# Information on the Anaplan API v2 can be found here: https://help.anaplan.com/perform-an-import-action-5ef6eaab-72d9-43de-91b3-3f4dcc4711e2

# This script uses Python 3 and assumes that you have the following modules
# installed: requests, os, json

import json
import os

import requests
from auth import get_auth_response, get_header

base_url = "https://api.anaplan.com/2/0"


def transfer_data(
    wGuid: str, mGuid: str, username: str, password: str, fileData: dict, data: str
):

    auth_response = get_auth_response(username=username, password=password)
    import_header = get_header(auth_response=auth_response)

    fileID = fileData["id"]

    # Set chunk count to 1
    setChunkSize = requests.post(
        url=f"{base_url}/workspaces/{wGuid}/models/{mGuid}/files/{fileID}",
        headers=import_header,
        json={"chunkCount": 1},
    )
    if setChunkSize.ok:
        print("Set chunk size successful.")
    else:
        print(
            "There was an issue with setting the chunk size: "
            + str(setChunkSize.status_code)
        )
        raise Exception("Noe gikk galt...")

    # Upload file
    putHeaders = import_header
    putHeaders["Content-Type"] = "application/octet-stream"

    dataFile = data
    chunkID = "0"

    fileUpload = requests.put(
        url=f"{base_url}/workspaces/{wGuid}/models/{mGuid}/files/{fileID}/chunks/{chunkID}",
        headers=putHeaders,
        data=(dataFile),
    )

    # Send post call to mark ulpoad as complete # NB: fungerer ikke!!
    # url = f"https://api.anaplan.com/2/0/workspaces/{wGuid}/models/{mGuid}/files/{fileID}/complete"
    # taskComplete = requests.post(
    #    url,
    #    headers=import_header,
    # )
    # print(f"Mark upload as complete: {taskComplete.json()}")

    if fileUpload.ok:
        print("File upload successful.")
    else:
        print(
            "There was an issue with your file upload: " + str(fileUpload.status_code)
        )
        raise Exception("Noe gikk galt...")
