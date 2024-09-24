# This script uploads a file in a single chunk.

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

    # Send post call to mark ulpoad as complete
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


if __name__ == "__main__":

    from import_data import import_data

    wGuid = "8a868cd985f53e7701860542f59e276e"
    mGuid = "54E5A1B9C12D4816B2D1876CFD9D7C84"
    username = os.environ["ANAPLAN_USR"]
    password = os.environ["ANAPLAN_PWD"]
    fileData = {
        "id": "113000000035",
        "name": "agg_hovedbok_posteringer_all_mnd_snowflake.csv",
    }

    with open("testfil-regnskap.csv", "r") as f:
        testfile = f.read().encode("UTF-8")

    transfer_data(
        wGuid=wGuid,
        mGuid=mGuid,
        username=username,
        password=password,
        fileData=fileData,
        data=testfile,
    )

    import_hierarchy_data = {
        "id": "112000000053",
        "name": "Regnskap Flat from agg_hovedbok_posteringer_all_mnd_snowflak",
    }

    import_module_data = {
        "id": "112000000054",
        "name": "Regnskap from agg_hovedbok_posteringer_all_mnd_snowflake.csv",
    }

    import_data(wGuid, mGuid, username, password, import_hierarchy_data)
    import_data(wGuid, mGuid, username, password, import_module_data)
