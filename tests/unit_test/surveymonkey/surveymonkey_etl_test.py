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

    def test_should_extract_modified_timestamp(self):
        result = get_bq_json_for_survey_questions_response_json(
            {**DEFAULT_SURVEY_QUESTIONS_RESPONSE_JSON, "date_modified": "2021-11-19 00:00:00"}
        )
        assert result["modified_timestamp"] == "2021-11-19 00:00:00"

    def test_should_ignore_other_field(self):
        result = get_bq_json_for_survey_questions_response_json(
            {**DEFAULT_SURVEY_QUESTIONS_RESPONSE_JSON, "other": "OTHER"}
        )
        assert result.get("other") is None

    def test_should_extract_question_answer_options_even_answers_field_not_exist(self):
        result = get_bq_json_for_survey_questions_response_json({
            **DEFAULT_SURVEY_QUESTIONS_RESPONSE_JSON,
            "pages": [{
                "questions": [{
                    "id": "Q_ID",
                    "headings": [{
                        "heading": "Is this the question?"
                    }],
                    "family": "matrix",
                    "subtype": "rating",
                }]
            }]
        })
        assert result["questions"] == [
            {
                "question_id": "Q_ID",
                "question_title": "Is this the question?",
                "question_type": "matrix",
                "question_subtype": "rating",
                "question_answers": [{}]
            }
        ]

    def test_should_extract_question_answers(self):
        result = get_bq_json_for_survey_questions_response_json({
            **DEFAULT_SURVEY_QUESTIONS_RESPONSE_JSON,
            "pages": [{
                "questions": [{
                    "id": "Q_ID",
                    "headings": [{
                        "heading": "Is this the question?"
                    }],
                    "family": "matrix",
                    "subtype": "rating",
                    "answers": {
                        "choices": [{
                            "id": "CHOICE_ID",
                            "text": "This is the choice"
                        }],
                        "other": {
                            "id": "OTHER_ID",
                            "text": "This is the other option",
                            "type": "OTHER_TYPE"
                        },
                        "rows": [{
                            "id": "ROW_ID",
                            "text": "This is the row",
                            "weight": 3
                        }],
                        "cols": [{
                            "id": "COL_ID",
                            "text": "This is the col",
                            "type": "COL_TYPE",
                            "weight": 100
                        }]
                    }
                }]
            }]
        })
        assert result["questions"] == [
            {
                "question_id": "Q_ID",
                "question_title": "Is this the question?",
                "question_type": "matrix",
                "question_subtype": "rating",
                "question_answers": [{
                    "choices": [{
                        "id": "CHOICE_ID",
                        "text": "This is the choice",
                        "type": "",
                        "weight": -99
                    }],
                    "other": [{
                        "id": "OTHER_ID",
                        "text": "This is the other option",
                        "type": "OTHER_TYPE",
                        "weight": -99
                    }],
                    "rows": [{
                        "id": "ROW_ID",
                        "text": "This is the row",
                        "type": "",
                        "weight": 3
                    }],
                    "cols": [{
                        "id": "COL_ID",
                        "text": "This is the col",
                        "type": "COL_TYPE",
                        "weight": 100
                    }],

                }]
            }
        ]

    def test_should_extract_question_answers_if_there_is_more_than_one(self):
        result = get_bq_json_for_survey_questions_response_json({
            **DEFAULT_SURVEY_QUESTIONS_RESPONSE_JSON,
            "pages": [{
                "questions": [{
                    "id": "Q_ID",
                    "headings": [{
                        "heading": "Is this the question?"
                    }],
                    "family": "matrix",
                    "subtype": "rating",
                    "answers": {
                        "choices": [{
                            "id": "CHOICE_ID",
                            "text": "This is the choice"
                        }, {
                            "id": "CHOICE_ID_2",
                            "text": "This is the second choice"
                        }]
                    }
                }]
            }]
        })
        assert result["questions"] == [
            {
                "question_id": "Q_ID",
                "question_title": "Is this the question?",
                "question_type": "matrix",
                "question_subtype": "rating",
                "question_answers": [{
                    "choices": [{
                        "id": "CHOICE_ID",
                        "text": "This is the choice",
                        "type": "",
                        "weight": -99
                    }, {
                        "id": "CHOICE_ID_2",
                        "text": "This is the second choice",
                        "type": "",
                        "weight": -99
                    }]
                }]
            }
        ]

    def test_should_handle_if_there_is_no_id_for_the_option(self):
        result = get_bq_json_for_survey_questions_response_json({
            **DEFAULT_SURVEY_QUESTIONS_RESPONSE_JSON,
            "pages": [{
                "questions": [{
                    "id": "Q_ID",
                    "headings": [{
                        "heading": "Is this the question?"
                    }],
                    "family": "matrix",
                    "subtype": "rating",
                    "answers": {
                        "other": {
                            "text": "It is a text"
                        }
                    }
                }]
            }]
        })
        assert result["questions"] == [
            {
                "question_id": "Q_ID",
                "question_title": "Is this the question?",
                "question_type": "matrix",
                "question_subtype": "rating",
                "question_answers": [{
                    "other": [{
                            "id": "",
                            "text": "It is a text",
                            "type": "",
                            "weight": -99
                        }]
                }]
            }
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

    def test_should_extract_total_time_as_total_time_spent_in_secs(self):
        result = get_bq_json_for_survey_answers_response_json(
            {**DEFAULT_SURVEY_ANSWERS_RESPONSE_JSON, "total_time": 123}
        )
        assert result["total_time_spent_in_secs"] == 123

    def test_should_extract_modified_timestamp(self):
        result = get_bq_json_for_survey_answers_response_json(
            {**DEFAULT_SURVEY_ANSWERS_RESPONSE_JSON, "date_modified": "2021-11-19 00:00:00"}
        )
        assert result["modified_timestamp"] == "2021-11-19 00:00:00"

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
