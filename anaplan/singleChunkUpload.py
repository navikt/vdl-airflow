# This script uploads a file in a single chunk.

# This script assumes you know your workspaceGuid, modelGuid, and file metadata.
# If you do not have this information, please run 'getWorkspaces.py',
# 'getModels.py', and 'getFiles.py' and retrieve this information from the
# resulting json files.

# If you are using certificate authentication, this script assumes you have
# converted your Anaplan certificate to PEM format, and that you know the
# Anaplan account email associated with that certificate.

# This script uses Python 3 and assumes that you have the following modules
# installed: requests, base64, json

import base64

import requests


def transfer_data(
    wGuid: str, mGuid: str, username: str, password: str, fileData: dict, data: str
):
    user = "Basic " + str(
        base64.b64encode((f"{username}:{password}").encode("utf-8")).decode("utf-8")
    )

    url = (
        f"https://api.anaplan.com/1/3/workspaces/{wGuid}/models/{mGuid}/"
        + f'files/{fileData["id"]}'
    )

    putHeaders = {"Authorization": user, "Content-Type": "application/octet-stream"}

    dataFile = data

    fileUpload = requests.put(url, headers=putHeaders, data=(dataFile))
    if fileUpload.ok:
        print("File Upload Successful.")
    else:
        print(
            "There was an issue with your file upload: " + str(fileUpload.status_code)
        )
        raise Exception("Noe gikk galt...")
