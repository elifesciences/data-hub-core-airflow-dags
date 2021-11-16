import datetime
import logging
import requests

LOGGER = logging.getLogger(__name__)


def get_current_timestamp():
    return datetime.datetime.utcnow().isoformat()


def get_surveymonkey_api_headers(access_token: str) -> dict:
    return {
        'Accept': "application/json",
        'Authorization': f"Bearer {access_token}"
    }


def get_survey_list(access_token: str) -> list:
    headers=get_surveymonkey_api_headers(access_token)
    response = requests.get('https://api.surveymonkey.com/v3/surveys', headers=headers)
    response.raise_for_status()
    survey_list = response.json()["data"]
    for survey in survey_list:
        survey["imported_timestamp"] = get_current_timestamp()
    return survey_list
