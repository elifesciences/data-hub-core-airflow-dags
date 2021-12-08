import datetime
import logging
from typing import Iterable, List, Union
import requests

LOGGER = logging.getLogger(__name__)


def get_current_timestamp():
    return datetime.datetime.utcnow().isoformat()


def get_surveymonkey_api_headers(access_token: str) -> dict:
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }


def get_survey_list(access_token: str) -> list:
    headers = get_surveymonkey_api_headers(access_token)
    response = requests.get("https://api.surveymonkey.com/v3/surveys", headers=headers)
    response.raise_for_status()
    survey_list = response.json()["data"]
    for survey in survey_list:
        survey["imported_timestamp"] = get_current_timestamp()
    return survey_list


def get_survey_question_details(access_token: str, survey_id: str) -> list:
    headers = get_surveymonkey_api_headers(access_token)
    response = requests.get(
        f"https://api.surveymonkey.com/v3/surveys/{survey_id}/details",
        headers=headers
    )
    response.raise_for_status()
    reponse_json = response.json()
    return reponse_json


def parse_answer_options_in_question_answer_json(
    question_answer_response_json: dict
) -> Union[dict, List[dict]]:
    if isinstance(question_answer_response_json, list):
        answer_options = question_answer_response_json
    else:
        answer_options = [question_answer_response_json]
    return [
        {
            key: value
            for key, value in answer_option.items()
            if key in {"id", "text", "type", "weight"} and value is not None
        }
        for answer_option in answer_options
    ]


def parse_answers_in_question_json(question_response_json: dict) -> dict:
    result = {}
    if "answers" in question_response_json:
        for key in ["choices", "other", "rows", "cols"]:
            if key in question_response_json["answers"]:
                result[key] = parse_answer_options_in_question_answer_json(
                    question_response_json["answers"][key]
                )
    return result


def get_bq_json_for_survey_questions_response_json(
    survey_response_json: dict
) -> dict:
    return {
        "title": survey_response_json["title"],
        "survey_id": survey_response_json["id"],
        "response_count": survey_response_json["response_count"],
        "modified_timestamp": survey_response_json["date_modified"],
        "questions": [
            {
                "question_id": question_response_json["id"],
                "question_title": question_response_json["headings"][0]["heading"],
                "question_type": question_response_json["family"],
                "question_subtype": question_response_json["subtype"],
                "question_answers": parse_answers_in_question_json(question_response_json)
            }
            for page_reponse_json in survey_response_json["pages"]
            for question_response_json in page_reponse_json["questions"]
        ],
        "imported_timestamp": get_current_timestamp()
    }


def get_survey_answers(
    access_token: str,
    page_url: str
) -> dict:
    headers = get_surveymonkey_api_headers(access_token)
    response = requests.get(page_url, headers=headers)
    response.raise_for_status()
    return response.json()


def iter_survey_answers(access_token: str, survey_id: str) -> Iterable[dict]:
    response_json = get_survey_answers(
        access_token,
        f"https://api.surveymonkey.com/v3/surveys/{survey_id}/responses/bulk/?per_page=100"
    )
    if not response_json.get("data"):
        LOGGER.info("No answers for the survey: %s", survey_id)
    yield from response_json["data"]
    while "next" in response_json["links"]:
        response_json = (
            get_survey_answers(access_token, response_json["links"]["next"])
        )
        yield from response_json["data"]


def parse_answers_part_in_survey_answers_response(question_response_json: dict):
    result = {}
    if "answers" in question_response_json:
        for answer in question_response_json["answers"]:
            for key in ["choice_id", "row_id", "col_id", "other_id", "text"]:
                if key in answer:
                    result[key] = answer[key]
    return result


def get_bq_json_for_survey_answers_response_json(
    survey_response_json: dict
) -> dict:
    return {
        "survey_answer_id": survey_response_json["id"],
        "survey_id": survey_response_json["survey_id"],
        "response_status": survey_response_json["response_status"],
        "total_time_spent_in_secs": survey_response_json["total_time"],
        "modified_timestamp": survey_response_json["date_modified"],
        "questions": [
            {
                "question_id": question_response_json["id"],
                "answers": [parse_answers_part_in_survey_answers_response(question_response_json)]
            }
            for page_response_json in survey_response_json["pages"]
            for question_response_json in page_response_json["questions"]
        ],
        "imported_timestamp": get_current_timestamp()
    }


def iter_formated_survey_user_answers(
    access_token: str,
    survey_id: str
) -> Iterable:
    survey_answers_iterable = iter_survey_answers(
        access_token=access_token,
        survey_id=survey_id
    )
    for user_survey_answers in survey_answers_iterable:
        yield get_bq_json_for_survey_answers_response_json(user_survey_answers)
