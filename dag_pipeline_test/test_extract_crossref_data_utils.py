from data_pipeline.crossref_event_data.etl_crossref_event_data_util import (
    get_last_run_day_from_cloud_storage,
    convert_bq_schema_field_list_to_dict,
    semi_clean_crossref_record,
    write_result_to_file_get_latest_record_timestamp,
    etl_crossref_data,
)
import data_pipeline.crossref_event_data.etl_crossref_event_data_util as etl_crossref_event_data_util_module
import pytest
from unittest.mock import patch

import datetime


@pytest.fixture(name="mock_download_s3_object")
def _download_s3_object(latest_date, test_download_exception: bool = False):
    with patch.object(
        etl_crossref_event_data_util_module, "download_s3_object"
    ) as mock:
        mock.return_value = latest_date
        if test_download_exception:
            mock.side_effect = BaseException
            mock.return_value = (
                etl_crossref_event_data_util_module.CROSSREF_DATA_COLLECTION_BEGINNING
            )
        yield mock


@pytest.fixture(name="mock_open_file")
def _open():
    with patch.object(etl_crossref_event_data_util_module, "open") as mock:
        yield mock


@pytest.fixture(name="mock_download_crossref")
def _get_crossref_data_single_page():
    with patch.object(
        etl_crossref_event_data_util_module, "get_crossref_data_single_page"
    ) as mock:
        test_data = TestData()
        mock.return_value = (None, test_data.get_downloaded_crossref_data())
        yield mock


class TestExtractCrossreData:
    def test_write_result_to_file_get_latest_record_timestamp(self, mock_open_file):
        test_data = TestData()
        max_timestamp = test_data.get_max_timestamp()
        result = write_result_to_file_get_latest_record_timestamp(
            test_data.get_data(),
            "tempfileloc",
            previous_latest_timestamp=datetime.datetime.strptime(
                "2000-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"
            ),
            imported_timestamp_key=test_data.data_imported_timestamp_key,
            imported_timestamp=test_data.data_imported_timestamp,
            schema=test_data.source_data_schema,
        )
        mock_open_file.assert_called_with("tempfileloc", "a")
        assert max_timestamp == result

    def test_etl_crossref_data(self, mock_download_crossref, mock_open_file):
        test_data = TestData()
        result = etl_crossref_data(
            base_crossref_url="base_crossref_url",
            from_date_collected_as_string="from_date_collected_as_string",
            publisher_id="pub_id",
            message_key=test_data.data_downloaded_message_key,
            event_key=test_data.data_downloaded_event_key,
            imported_timestamp_key=test_data.data_imported_timestamp_key,
            imported_timestamp=test_data.data_imported_timestamp,
            full_temp_file_location="temp_file_loc",
            schema=test_data.source_data_schema,
        )
        mock_open_file.assert_called_with("temp_file_loc", "a")
        assert result == test_data.get_max_timestamp()
        assert mock_download_crossref.called_with(
            base_crossref_url="base_crossref_url",
            from_date_collected_as_string="from_date_collected_as_string",
            publisher_id="publisher_id",
            message_key="message_key",
        )

    @pytest.mark.parametrize(
        "latest_date, number_of_prv_days, data_download_start_date, test_download_exception",
        [
            ("2019-10-23", 1, "2019-10-22", False),
            ("2016-09-23", 7, "2016-09-16", False),
            ("2016-09-23", 7, "2016-09-16", True),
        ],
    )
    def test_get_last_run_day_from_cloud_storage(
        self,
        mock_download_s3_object,
        latest_date,
        number_of_prv_days,
        data_download_start_date,
        test_download_exception,
    ):
        from_date = get_last_run_day_from_cloud_storage(
            "bucket", "object_key", number_of_prv_days
        )
        mock_download_s3_object.assert_called_with("bucket", "object_key")
        assert from_date == data_download_start_date

    def test_convert_bq_schema_field_list_to_dict(self):
        test_data = TestData()
        source_data = test_data.data_bq_schema_field_list_to_convert_to_dict
        expected_converted_data = (
            test_data.data_bq_schema_field_list_to_convert_to_dict_result
        )
        returned_data = convert_bq_schema_field_list_to_dict(source_data)

        assert returned_data == expected_converted_data

    def test_semi_clean_crossref_record(self):
        test_data = TestData()
        assert (
            test_data.test_data_all_field_present_result
            == semi_clean_crossref_record(
                test_data.test_data_all_field_present, test_data.source_data_schema
            )
        )

        assert (
            test_data.test_data_more_field_present_result
            == semi_clean_crossref_record(
                test_data.test_data_more_field_present, test_data.source_data_schema
            )
        )

        assert (
            test_data.test_data_non_parseable_timestamp_present_result
            == semi_clean_crossref_record(
                test_data.test_data_non_parseable_timestamp_present,
                test_data.source_data_schema,
            )
        )

        assert (
            test_data.test_data_some_field_present_result
            == semi_clean_crossref_record(
                test_data.test_data_some_field_present, test_data.source_data_schema
            )
        )


class TestData:
    def __init__(self):
        self.source_data_schema = [
            {"mode": "NULLABLE", "name": "_type", "type": "STRING"},
            {"mode": "NULLABLE", "name": "source_token", "type": "INTEGER"},
            {
                "fields": [
                    {"mode": "NULLABLE", "name": "auth_id", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "auth_name", "type": "STRING"},
                ],
                "mode": "REPEATABLE",
                "name": "repeatable_record",
                "type": "RECORD",
            },
            {
                "fields": [{"mode": "NULLABLE", "name": "id", "type": "STRING"},],
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
            "familyName": {"mode": "NULLABLE", "name": "familyName", "type": "STRING"},
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

    def get_max_timestamp(self):
        all_string_timestamp = [time.get("timestamp") for time in self.get_data()]
        all_timestamp = []
        for string_timestamp in all_string_timestamp:
            try:
                all_timestamp.append(
                    datetime.datetime.strptime(string_timestamp, "%Y-%m-%dT%H:%M:%SZ")
                )
            except BaseException:
                continue
        return max(all_timestamp)

    def get_expected_processed_crossref_test_data(self):
        data = [
            self.test_data_some_field_present_result,
            self.test_data_non_parseable_timestamp_present_result,
            self.test_data_all_field_present_result,
            self.test_data_more_field_present_result,
        ]
        y = []
        for x in data:
            x[self.data_imported_timestamp_key] = self.data_imported_timestamp
            y.extend(x)
        return y

    def get_downloaded_crossref_data(self):
        data_downloaded__with_event_key = dict()
        data_downloaded = dict()
        data_downloaded__with_event_key[
            self.data_downloaded_event_key
        ] = self.get_data()
        data_downloaded[
            self.data_downloaded_message_key
        ] = data_downloaded__with_event_key

        return data_downloaded
