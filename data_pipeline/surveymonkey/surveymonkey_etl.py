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
    headers = get_surveymonkey_api_headers(access_token)
    response = requests.get('https://api.surveymonkey.com/v3/surveys', headers=headers)
    response.raise_for_status()
    survey_list = response.json()["data"]
    for survey in survey_list:
        survey["imported_timestamp"] = get_current_timestamp()
    return survey_list


def get_bq_json_for_survey_response_json(survey_response_json: dict) -> dict:

    return {
        "title": survey_response_json["title"],
        "survey_id": survey_response_json["id"],
        "questions": [
            {
                "question_id": question_response_json["id"],
                "question_title": question_response_json["headings"][0]["heading"]
            }
            for page_reponse_json in survey_response_json["pages"]
            for question_response_json in page_reponse_json["questions"]
        ]
    }
