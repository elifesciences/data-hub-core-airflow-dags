from data_pipeline.surveymonkey.surveymonkey_etl import (
    get_bq_json_for_survey_questions_response_json,
    get_bq_json_for_survey_answers_response_json
)

DEFAULT_SURVEY_QUESTIONS_RESPONSE_JSON = {
    "title": "DEFAULT_TITLE",
    "id": "DEFAULT_ID",
    "pages": [],
    "imported_timestamp": "",
    "response_count": 0,
    "date_modified": "DEFAULT_DATE"
}

DEFAULT_SURVEY_ANSWERS_RESPONSE_JSON = {
    "id": "DEFAULT_ID",
    "survey_id": "DEFAULT_SURVEY_ID",
    "response_status": "DEFAULT_STATUS",
    "total_time": 0,
    "date_modified": "DEFAULT_DATE",
    "imported_timestamp": "",
    "pages": []
}


class TestGetBqJsonForSurveyQuestionsResponseJson():

    def test_should_extract_title(self):
        result = get_bq_json_for_survey_questions_response_json(
            {**DEFAULT_SURVEY_QUESTIONS_RESPONSE_JSON, "title": "TITLE"}
        )
        assert result["title"] == "TITLE"

    def test_should_extract_id_as_survey_id(self):
        result = get_bq_json_for_survey_questions_response_json(
            {**DEFAULT_SURVEY_QUESTIONS_RESPONSE_JSON, "id": "ID"}
        )
        assert result["survey_id"] == "ID"

    def test_should_extract_response_count(self):
        result = get_bq_json_for_survey_questions_response_json(
            {**DEFAULT_SURVEY_QUESTIONS_RESPONSE_JSON, "response_count": 13}
        )
        assert result["response_count"] == 13

    def test_should_extract_date_modified(self):
        result = get_bq_json_for_survey_questions_response_json(
            {**DEFAULT_SURVEY_QUESTIONS_RESPONSE_JSON, "date_modified": "2021-11-19"}
        )
        assert result["date_modified"] == "2021-11-19"

    def test_should_ignore_other_field(self):
        result = get_bq_json_for_survey_questions_response_json(
            {**DEFAULT_SURVEY_QUESTIONS_RESPONSE_JSON, "other": "OTHER"}
        )
        assert result.get("other") is None

    def test_should_extract_question(self):
        result = get_bq_json_for_survey_questions_response_json({
            **DEFAULT_SURVEY_QUESTIONS_RESPONSE_JSON,
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


class TestGetBqJsonForSurveyAnswersResponseJson():

    def test_should_extract_id_as_survey_answer_id(self):
        result = get_bq_json_for_survey_answers_response_json(
            {**DEFAULT_SURVEY_ANSWERS_RESPONSE_JSON, "id": "ANSWER_ID"}
        )
        assert result["survey_answer_id"] == "ANSWER_ID"

    def test_should_extract_survey_id(self):
        result = get_bq_json_for_survey_answers_response_json(
            {**DEFAULT_SURVEY_ANSWERS_RESPONSE_JSON, "survey_id": "SURVEY_ID"}
        )
        assert result["survey_id"] == "SURVEY_ID"

    def test_should_extract_response_status(self):
        result = get_bq_json_for_survey_answers_response_json(
            {**DEFAULT_SURVEY_ANSWERS_RESPONSE_JSON, "response_status": "completed"}
        )
        assert result["response_status"] == "completed"

    def test_should_extract_total_time_as_total_time_spent(self):
        result = get_bq_json_for_survey_answers_response_json(
            {**DEFAULT_SURVEY_ANSWERS_RESPONSE_JSON, "total_time": 123}
        )
        assert result["total_time_spent"] == 123

    def test_should_extract_date_modified(self):
        result = get_bq_json_for_survey_answers_response_json(
            {**DEFAULT_SURVEY_ANSWERS_RESPONSE_JSON, "date_modified": "2021-11-19"}
        )
        assert result["date_modified"] == "2021-11-19"

    def test_should_extract_question_and_its_answers(self):
        result = get_bq_json_for_survey_answers_response_json({
            **DEFAULT_SURVEY_ANSWERS_RESPONSE_JSON,
            "pages": [{
                "questions": [{
                    "id": "Q_ID",
                    "answers": []
                }]
            }]
        })
        assert result["questions"] == [
            {"question_id": "Q_ID", "answers": []}
        ]

    def test_should_include_all_existing_answer_id_keys(self):
        result = get_bq_json_for_survey_answers_response_json({
            **DEFAULT_SURVEY_ANSWERS_RESPONSE_JSON,
            "pages": [{
                "questions": [{
                    "id": "Q_ID",
                    "answers": [{
                        "choice_id": "CHOICE_ID_1",
                        "row_id": "ROW_ID_1",
                        "col_id": "COL_ID_1",
                        "other_id": "OTHER_ID_1",
                        "text": "TEXT_1"
                    }]
                }]
            }]
        })
        assert result["questions"] == [{
            "question_id": "Q_ID",
            "answers": [{
                "choice_id": "CHOICE_ID_1",
                "row_id": "ROW_ID_1",
                "col_id": "COL_ID_1",
                "other_id": "OTHER_ID_1",
                "text": "TEXT_1"
            }]
        }]

    def test_should_include_all_id_keys_even_they_dont_exist_in_reponse(self):
        result = get_bq_json_for_survey_answers_response_json({
            **DEFAULT_SURVEY_ANSWERS_RESPONSE_JSON,
            "pages": [{
                "questions": [{
                    "id": "Q_ID",
                    "answers": [{
                    }]
                }]
            }]
        })
        assert result["questions"] == [{
            "question_id": "Q_ID",
            "answers": [{
                "choice_id": None,
                "row_id": None,
                "col_id": None,
                "other_id": None,
                "text": None
            }]
        }]
