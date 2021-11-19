from data_pipeline.surveymonkey.surveymonkey_etl import (
    get_bq_json_for_survey_questions_response_json
)

DEFAULT_SURVEY_RESPONSE_JSON = {
    "title": "DEFAULT_TITLE",
    "id": "DEFAULT_ID",
    "pages": [],
    "imported_timestamp": "",
    "response_count": "",
    "date_modified": ""
}


class TestGetBqJsonForSurveyQuestionsResponseJson():

    def test_should_extract_title(self):
        result = get_bq_json_for_survey_questions_response_json(
            {**DEFAULT_SURVEY_RESPONSE_JSON, "title": "TITLE"}
        )
        assert result["title"] == "TITLE"

    def test_should_ignore_other_field(self):
        result = get_bq_json_for_survey_questions_response_json(
            {**DEFAULT_SURVEY_RESPONSE_JSON, "other": "OTHER"}
        )
        assert result.get("other") is None

    def test_should_extract_id_as_survey_id(self):
        result = get_bq_json_for_survey_questions_response_json(
            {**DEFAULT_SURVEY_RESPONSE_JSON, "id": "ID"}
        )
        assert result["survey_id"] == "ID"

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
