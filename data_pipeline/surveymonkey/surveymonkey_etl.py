import http.client
import json
import logging

LOGGER = logging.getLogger(__name__)


def get_connection_surveymonkey_api() -> http:
    return http.client.HTTPSConnection("api.surveymonkey.com")


def get_surveymonkey_api_headers(access_token: str) -> dict:
    return {
        'Accept': "application/json",
        'Authorization': f"Bearer {access_token}"
        }


def get_survey_list(access_token: str) -> list:
    conn = get_connection_surveymonkey_api()
    headers = get_surveymonkey_api_headers(access_token)

    conn.request("GET", "/v3/surveys", headers=headers)

    res = conn.getresponse()
    surveys = res.read().decode("utf-8")

    return json.loads(surveys)["data"]
