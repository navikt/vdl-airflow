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

import requests
import base64
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from anaplan.get_data import get_data


def transfer_data():
    # Insert your workspace Guid
    wGuid = "8a868cda860a533a0186334e91805794"
    # Insert your model Guid
    mGuid = "A07AB2A8DBA24E13B8A6E9EBCDB6235E"
    # Insert the Anaplan account email being used
    username = "virksomhetsdatalaget@nav.no"
    # Replace with your file metadata
    fileData = {
        "id": "113000000033",
        "name": "dim_artskonti.csv",
        "chunkCount": 1,
        "delimiter": '"',
        "encoding": "UTF-8",
        "firstDataRow": 2,
        "format": "txt",
        "headerRow": 1,
        "separator": ",",
    }

    # If using cert auth, replace cert.pem with your pem converted certificate
    # filename. Otherwise, remove this line.
    # cert = open("cert.pem").read()

    # If using basic auth, insert your password. Otherwise, remove this line.
    password = Variable.get("anaplan_password")

    # Uncomment your authentication method (cert or basic). Remove the other.
    # user = 'AnaplanCertificate ' + str(base64.b64encode((
    #       f'{username}:{cert}').encode('utf-8')).decode('utf-8'))

    user = "Basic " + str(
        base64.b64encode((f"{username}:{password}").encode("utf-8")).decode("utf-8")
    )

    url = (
        f"https://api.anaplan.com/1/3/workspaces/{wGuid}/models/{mGuid}/"
        + f'files/{fileData["id"]}'
    )

    putHeaders = {"Authorization": user, "Content-Type": "application/octet-stream"}

    with SnowflakeHook().get_cursor() as cursor:
        query =  """
            select *
            from reporting.microstrategy.dim_artskonti
            where
                er_budsjetterbar = 1 and
                artskonti_segment_kode_niva_1 is not null
            """
        dataFile = get_data(query, cursor).encode("utf-8")

    fileUpload = requests.put(url, headers=putHeaders, data=(dataFile))
    if fileUpload.ok:
        print("File Upload Successful.")
    else:
        print(
            "There was an issue with your file upload: " + str(fileUpload.status_code)
        )
        raise Exception("Noe gikk galt...")


transfer_data()
