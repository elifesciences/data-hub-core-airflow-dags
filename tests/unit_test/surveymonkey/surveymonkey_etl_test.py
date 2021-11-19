from data_pipeline.surveymonkey.surveymonkey_etl import (
    get_bq_json_for_survey_questions_response_json
)

DEFAULT_SURVEY_RESPONSE_JSON = {
    "title": "DEFAULT_TITLE",
    "id": "DEFAULT_ID",
    "pages": [],
    "imported_timestamp": "",
    "response_count": 0,
    "date_modified": "DEFAULT_DATE"
}


class TestGetBqJsonForSurveyQuestionsResponseJson():

    def test_should_extract_title(self):
        result = get_bq_json_for_survey_questions_response_json(
            {**DEFAULT_SURVEY_RESPONSE_JSON, "title": "TITLE"}
        )
        assert result["title"] == "TITLE"

    def test_should_extract_id_as_survey_id(self):
        result = get_bq_json_for_survey_questions_response_json(
            {**DEFAULT_SURVEY_RESPONSE_JSON, "id": "ID"}
        )
        assert result["survey_id"] == "ID"

    def test_should_extract_response_count(self):
        result = get_bq_json_for_survey_questions_response_json(
            {**DEFAULT_SURVEY_RESPONSE_JSON, "response_count": 13}
        )
        assert result["response_count"] == 13

    def test_should_extract_date_modified(self):
        result = get_bq_json_for_survey_questions_response_json(
            {**DEFAULT_SURVEY_RESPONSE_JSON, "date_modified": "2021-11-19"}
        )
        assert result["date_modified"] == "2021-11-19"

    def test_should_ignore_other_field(self):
        result = get_bq_json_for_survey_questions_response_json(
            {**DEFAULT_SURVEY_RESPONSE_JSON, "other": "OTHER"}
        )
        assert result.get("other") is None

    def test_shoud_extract_question(self):
        result = get_bq_json_for_survey_questions_response_json({
            **DEFAULT_SURVEY_RESPONSE_JSON,
            "pages": [{
                "questions": [{
                    "id": "Q_ID",
                    "headings": [{
                        "heading": "Is this the question?"
                    }]
                }]
            }]
        })
        assert result["questions"] == [
            {"question_id": "Q_ID", "question_title": "Is this the question?"}
        ]
