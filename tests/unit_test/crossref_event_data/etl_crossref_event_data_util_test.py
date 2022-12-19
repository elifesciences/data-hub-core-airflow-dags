import datetime
import json
from unittest.mock import patch

import pytest

import data_pipeline.crossref_event_data.etl_crossref_event_data_util \
    as etl_crossref_event_data_util_module
from data_pipeline.crossref_event_data.etl_crossref_event_data_util import (
    get_new_data_download_start_date_from_cloud_storage,
    convert_bq_schema_field_list_to_dict,
    semi_clean_crossref_record,
    preprocess_json_record,
    get_latest_json_record_list_timestamp,
    etl_crossref_data_return_latest_timestamp,
    convert_datetime_to_date_string,
)
from data_pipeline.utils.pipeline_file_io import (
    iter_write_jsonl_to_file
)
from data_pipeline.utils import pipeline_file_io as pipeline_file_io_module


@pytest.fixture(name="mock_download_s3_object")
def _download_s3_object(publisher_latest_date,
                        test_download_exception: bool = False):
    with patch.object(
            etl_crossref_event_data_util_module,
            "download_s3_json_object"
    ) as mock:
        mock.return_value = publisher_latest_date
        if test_download_exception:
            mock.side_effect = BaseException
            mock.return_value = (
                etl_crossref_event_data_util_module.
                EtlModuleConstant.DEFAULT_DATA_COLLECTION_START_DATE)
        yield mock


@pytest.fixture(name="mock_open_file")
def _open():
    with patch.object(pipeline_file_io_module, "open") as mock:
        yield mock


@pytest.fixture(name="mock_download_crossref")
def _get_crossref_data_single_page():
    with patch.object(
            etl_crossref_event_data_util_module,
            "get_crossref_data_single_page"
    ) as mock:
        test_data = UnitTestData()
        mock.return_value = (None, test_data.get_downloaded_crossref_data())
        yield mock


def test_should_get_latest_timestamp_after_data_processing():
    test_data = UnitTestData()
    max_timestamp = test_data.get_max_timestamp()
    results = (
        preprocess_json_record(
            test_data.get_data(),
            test_data.data_imported_timestamp_key,
            test_data.data_imported_timestamp,
            test_data.source_data_schema)
    )
    latest_collected_record_timestamp = (
        get_latest_json_record_list_timestamp(
            results,
            datetime.datetime.strptime(
                "2000-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"
            )
        )
    )

    assert max_timestamp == latest_collected_record_timestamp


def test_should_write_result_to_file(mock_open_file):
    test_data = UnitTestData()
    results = (
        preprocess_json_record(
            test_data.get_data(),
            test_data.data_imported_timestamp_key,
            test_data.data_imported_timestamp,
            test_data.source_data_schema)
    )
    written_json = iter_write_jsonl_to_file(
        results,
        "tempfileloc"
    )
    (
        get_latest_json_record_list_timestamp(
            written_json,
            datetime.datetime.strptime(
                "2000-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"
            )
        )
    )
    mock_open_file.assert_called_with("tempfileloc", "a", encoding='UTF-8')


def test_should_download_crossref_event_data(
        mock_download_crossref, mock_open_file
):
    test_data = UnitTestData()
    publisher_id = "pub_id"
    result = etl_crossref_data_return_latest_timestamp(
        base_crossref_url="base_crossref_url",
        latest_journal_download_date={publisher_id:
                                      "2000-01-01"},
        journal_doi_prefixes=[publisher_id],
        message_key=test_data.data_downloaded_message_key,
        event_key=test_data.data_downloaded_event_key,
        imported_timestamp_key=test_data.data_imported_timestamp_key,
        imported_timestamp=test_data.data_imported_timestamp,
        full_temp_file_location="temp_file_loc",
        schema=test_data.source_data_schema,
    )
    result = json.loads(result)
    mock_open_file.assert_called_with("temp_file_loc", "a", encoding='UTF-8')
    assert result == test_data.get_publisher_max_timestamp(publisher_id)
    assert mock_download_crossref.called_with(
        base_crossref_url="base_crossref_url",
        from_date_collected_as_string="from_date_collected_as_string",
        publisher_id=publisher_id,
        message_key="message_key",
    )


# pylint: disable=unused-argument
class TestGetNewDataDownloadStartDateFromCloudStorage:
    @pytest.mark.parametrize(
        "publisher_latest_date, number_of_prv_days, "
        "data_download_start_date",
        [
            ({"A": "2019-10-23"}, 1, {"A": "2019-10-22"}),
            ({"A": "2016-09-23"}, 7, {"A": "2016-09-16"})
        ],
    )
    def test_should_get_last_data_collection_date_from_cloud_storage(
        self,
        mock_download_s3_object,
        number_of_prv_days,
        data_download_start_date,
    ):
        from_date = get_new_data_download_start_date_from_cloud_storage(
            "bucket", "object_key", number_of_prv_days
        )
        mock_download_s3_object.assert_called_with("bucket", "object_key")
        assert from_date == data_download_start_date


def test_should_convert_bq_schema_field_list_to_dict():
    test_data = UnitTestData()
    source_data = test_data.data_bq_schema_field_list_to_convert_to_dict
    expected_converted_data = (
        test_data.data_bq_schema_field_list_to_convert_to_dict_result
    )
    returned_data = convert_bq_schema_field_list_to_dict(source_data)

    assert returned_data == expected_converted_data


def test_extracted_data_when_schema_and_source_data_fields_are_similar():
    test_data = UnitTestData()
    assert (
        test_data.test_data_all_field_present_result ==
        semi_clean_crossref_record(
            test_data.test_data_all_field_present,
            test_data.source_data_schema
        )
    )


def test_extracted_data_when_source_data_has_more_fields_than_schema():
    test_data = UnitTestData()
    assert (
        test_data.test_data_more_field_present_result ==
        semi_clean_crossref_record(
            test_data.test_data_more_field_present,
            test_data.source_data_schema
        )
    )


def test_extracted_data_when_source_data_has_no_parseable_timestamp_string():
    test_data = UnitTestData()
    assert (
        test_data.test_data_non_parseable_timestamp_present_result
        == semi_clean_crossref_record(
            test_data.test_data_non_parseable_timestamp_present,
            test_data.source_data_schema,
        )
    )


def test_extracted_data_when_source_data_has_less_fields_than_schema():
    test_data = UnitTestData()
    assert (
        test_data.test_data_some_field_present_result ==
        semi_clean_crossref_record(
            test_data.test_data_some_field_present,
            test_data.source_data_schema
        )
    )


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

    # pylint: disable=broad-except
    def get_publisher_max_timestamp(self, publisher_id):
        """
        :param publisher_id:
        :return:
        """
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
        return {
            publisher_id:
            convert_datetime_to_date_string(max(all_timestamp))
        }

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
