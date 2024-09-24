import base64
import json

import requests

base_url = "https://api.anaplan.com/2/0"  # v2
auth_url = "https://auth.anaplan.com/token/authenticate"


class AnaplanAuthException(Exception):
    pass


def get_auth_response(username, password) -> requests.Response:
    user = "Basic " + str(
        base64.b64encode((f"{username}:{password}").encode("utf-8")).decode("utf-8")
    )
    auth_header = {"Authorization": user, "Content-Type": "application/json"}

    auth_response = requests.post(
        url=auth_url,
        headers=auth_header,
        data=json.dumps({"localeName": "en_US"}),
    )
    return auth_response


def get_header(auth_response: requests.Response):
    if not auth_response.ok:
        raise AnaplanAuthException(
            f"Authentication against Anaplan failed: {auth_response.text}"
        )
    token_value = auth_response.json()["tokenInfo"]["tokenValue"]
    import_headers = {
        "Authorization": f"AnaplanAuthToken {token_value}",
        "Content-Type": "application/json",
    }
    return import_headers
