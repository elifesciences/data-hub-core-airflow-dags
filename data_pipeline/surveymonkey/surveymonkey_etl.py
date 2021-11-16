import http.client
import json
import logging

LOGGER = logging.getLogger(__name__)

SURVEYMONKEY_ACCESS_TOKEN_ENV_VAR="SURVEYMONKEY_ACCESS_TOKEN"


def get_connection_surveymonkey_api() -> http:
    return http.client.HTTPSConnection("api.surveymonkey.com")


def get_surveymonkey_api_headers() -> dict:
    return {
        'Accept': "application/json",
        'Authorization': f"Bearer {SURVEYMONKEY_ACCESS_TOKEN_ENV_VAR}"
        }


def get_suvey_list() -> dict:
    conn = get_connection_surveymonkey_api()
    headers=get_surveymonkey_api_headers()

    conn.request("GET", "/v3/surveys", headers=headers)

    res = conn.getresponse()
    surveys = res.read().decode("utf-8")

    return json.loads(surveys)
