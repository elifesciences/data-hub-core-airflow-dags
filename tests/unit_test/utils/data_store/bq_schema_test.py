import datetime
from unittest.mock import patch

import pytest

import data_pipeline.utils.data_store.bq_schema \
    as etl_crossref_event_data_util_module
from data_pipeline.utils.data_store.bq_schema import (
    convert_bq_schema_field_list_to_dict
)
from data_pipeline.utils import pipeline_file_io as pipeline_file_io_module


@pytest.fixture(name="download_s3_object_as_string_or_file_not_found_error_mock")
def _download_s3_object_as_string_or_file_not_found_error_mock_mock(
    publisher_latest_date_dict: dict
):
    with patch.object(
        etl_crossref_event_data_util_module,
        "download_s3_object_as_string_or_file_not_found_error"
    ) as mock:
        mock.return_value = publisher_latest_date_dict
        yield mock


@pytest.fixture(name="mock_open_file")
def _open():
    with patch.object(pipeline_file_io_module, "open") as mock:
        yield mock


def test_should_convert_bq_schema_field_list_to_dict():
    test_data = UnitTestData()
    source_data = test_data.data_bq_schema_field_list_to_convert_to_dict
    expected_converted_data = (
        test_data.data_bq_schema_field_list_to_convert_to_dict_result
    )
    returned_data = convert_bq_schema_field_list_to_dict(source_data)

    assert returned_data == expected_converted_data


# pylint: disable=too-many-instance-attributes
class UnitTestData:
    def __init__(self):
        self.source_data_schema = [
            {"mode": "NULLABLE", "name": "_type", "type": "STRING"},
            {"mode": "NULLABLE", "name": "source_token", "type": "INTEGER"},
            {
                "fields": [
                    {"mode": "NULLABLE", "name": "auth_id", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "auth_name",
                     "type": "STRING"},
                ],
                "mode": "REPEATABLE",
                "name": "repeatable_record",
                "type": "RECORD",
            },
            {
                "fields": [
                    {"mode": "NULLABLE", "name": "id", "type": "STRING"}
                ],
                "mode": "NULLABLE",
                "name": "nullable_record",
                "type": "RECORD",
            },
            {"mode": "REPEATABLE", "name": "subj", "type": "STRING"},
            {"mode": "NULLABLE", "name": "timestamp", "type": "TIMESTAMP"},
        ]

        self.test_data_all_field_present = {
            "@type": "@type_value",
            "source-token": "source_token_value",
            "repeatable record": [
                {"auth id": "auth_id1", "auth_name": "auth_name1"},
                {"auth id": "auth_id2", "auth_name": "auth_name2"},
            ],
            "nullable record": {"id": "id_val"},
            "subj": ["subj1", "subj2"],
            "timestamp": "2019-10-08T02:03:22Z",
        }

        self.test_data_all_field_present_result = {
            "_type": "@type_value",
            "source_token": "source_token_value",
            "repeatable_record": [
                {"auth_id": "auth_id1", "auth_name": "auth_name1"},
                {"auth_id": "auth_id2", "auth_name": "auth_name2"},
            ],
            "nullable_record": {"id": "id_val"},
            "subj": ["subj1", "subj2"],
            "timestamp": "2019-10-08T02:03:22Z",
        }

        self.test_data_some_field_present = {
            "repeatable record": [
                {"auth id": "auth_id1", "auth_name": "auth_name1"},
                {"auth id": "auth_id2", "auth_name": "auth_name2"},
            ],
            "nullable record": {"id": "id_val"},
            "timestamp": "2018-10-08T02:03:22Z",
        }

        self.test_data_some_field_present_result = {
            "repeatable_record": [
                {"auth_id": "auth_id1", "auth_name": "auth_name1"},
                {"auth_id": "auth_id2", "auth_name": "auth_name2"},
            ],
            "nullable_record": {"id": "id_val"},
            "timestamp": "2018-10-08T02:03:22Z",
        }

        self.test_data_more_field_present = {
            "EXTRA FIELD NOT IN SCHEMA": "2019-10-08T01:59:59Z",
            "@type": "@type_value",
            "source-token": "source_token_value",
            "repeatable record": [
                {"auth id": "auth_id1", "auth_name": "auth_name1"},
                {"auth id": "auth_id2", "auth_name": "auth_name2"},
            ],
            "nullable record": {"id": "id_val"},
            "subj": ["subj1", "subj2"],
            "timestamp": "2017-10-08T02:03:22Z",
        }

        self.test_data_more_field_present_result = {
            "_type": "@type_value",
            "source_token": "source_token_value",
            "repeatable_record": [
                {"auth_id": "auth_id1", "auth_name": "auth_name1"},
                {"auth_id": "auth_id2", "auth_name": "auth_name2"},
            ],
            "nullable_record": {"id": "id_val"},
            "subj": ["subj1", "subj2"],
            "timestamp": "2017-10-08T02:03:22Z",
        }

        self.test_data_non_parseable_timestamp_present = {
            "@type": "@type_value",
            "source-token": "source_token_value",
            "repeatable record": [
                {"auth id": "auth_id1", "auth_name": "auth_name1"},
                {"auth id": "auth_id2", "auth_name": "auth_name2"},
            ],
            "nullable record": {"id": "id_val"},
            "subj": ["subj1", "subj2"],
            "timestamp": "NON PARSEABLE TIMESTAMP",
        }
        self.test_data_non_parseable_timestamp_present_result = {
            "_type": "@type_value",
            "source_token": "source_token_value",
            "repeatable_record": [
                {"auth_id": "auth_id1", "auth_name": "auth_name1"},
                {"auth_id": "auth_id2", "auth_name": "auth_name2"},
            ],
            "nullable_record": {"id": "id_val"},
            "subj": ["subj1", "subj2"],
            "timestamp": None,
        }

        self.data_bq_schema_field_list_to_convert_to_dict = [
            {"mode": "NULLABLE", "name": "_type", "type": "STRING"},
            {
                "fields": [
                    {"mode": "NULLABLE", "name": "_type", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "name", "type": "STRING"},
                ],
                "mode": "NULLABLE",
                "name": "affiliation",
                "type": "RECORD",
            },
            {"mode": "NULLABLE", "name": "familyName", "type": "STRING"},
        ]

        self.data_bq_schema_field_list_to_convert_to_dict_result = {
            "_type": {"mode": "NULLABLE", "name": "_type", "type": "STRING"},
            "affiliation": {
                "fields": [
                    {"mode": "NULLABLE", "name": "_type", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "name", "type": "STRING"},
                ],
                "mode": "NULLABLE",
                "name": "affiliation",
                "type": "RECORD",
            },
            "familyName": {"mode": "NULLABLE", "name": "familyName",
                           "type": "STRING"},
        }
        self.data_imported_timestamp = "2019-12-30T02:03:22Z"
        self.data_imported_timestamp_key = "test_data_imported_timestamp_key"
        self.data_downloaded_message_key = "message_key"
        self.data_downloaded_event_key = "event_key"

    def get_data(self):
        all_source_data_combined = [
            self.test_data_some_field_present,
            self.test_data_non_parseable_timestamp_present,
            self.test_data_all_field_present,
            self.test_data_more_field_present,
        ]
        return all_source_data_combined

    # pylint: disable=broad-except
    def get_max_timestamp(self):
        all_string_timestamp = [time.get("timestamp")
                                for time in self.get_data()]
        all_timestamp = []
        for string_timestamp in all_string_timestamp:
            try:
                all_timestamp.append(
                    datetime.datetime.strptime(
                        string_timestamp,
                        "%Y-%m-%dT%H:%M:%SZ"))
            except Exception:
                continue
        return max(all_timestamp)

    def get_expected_processed_crossref_test_data(self):
        data = [
            self.test_data_some_field_present_result,
            self.test_data_non_parseable_timestamp_present_result,
            self.test_data_all_field_present_result,
            self.test_data_more_field_present_result,
        ]
        modified_data = []
        for data_record in data:
            data_record[self.data_imported_timestamp_key] = \
                self.data_imported_timestamp
            modified_data.extend(data_record)
        return modified_data

    def get_downloaded_crossref_data(self):
        data_downloaded__with_event_key = {}
        data_downloaded = {}
        data_downloaded__with_event_key[
            self.data_downloaded_event_key
        ] = self.get_data()
        data_downloaded[
            self.data_downloaded_message_key
        ] = data_downloaded__with_event_key

        return data_downloaded
