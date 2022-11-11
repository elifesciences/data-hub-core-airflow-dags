import os
from collections import OrderedDict
from typing import Optional
from unittest.mock import patch
import pytest
import botocore

from data_pipeline.s3_csv_data import s3_csv_etl
from data_pipeline.s3_csv_data.s3_csv_etl import (
    get_record_metadata,
    transform_load_data,
    get_standardized_csv_header,
    process_record_list,
    get_csv_dict_reader,
    get_initial_state,
    get_stored_state,
    update_metadata_with_provenance,
    merge_record_with_metadata
)
from data_pipeline.s3_csv_data.s3_csv_config import (
    S3BaseCsvConfig
)
from data_pipeline.utils.record_processing import DEFAULT_PROCESSING_STEPS
from dags.s3_csv_import_pipeline import (
    DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE
)


# pylint: disable=trailing-whitespace
TEST_DOWNLOADED_SHEET = """'First Name', 'Last_Name', 'Age', 'Univ', 'Country','html_encoded', ''
'Michael', 'Bonbi', '7','Univ of California', 'United States','&pound;682m',
'Robert', 'Alfonso', '', 'Univ of Cambridge', 'France','test ampersand &amp;'
'Michael', 'Shayne', '', '', 'England',''
'Fred', 'Fredrick', '21', '', 'China',''
"""


@pytest.fixture(name="mock_os_stat", autouse=True)
def _os_stat():
    with patch.object(os, "stat") as mock:
        mock.return_value.st_size = 1
        yield mock


@pytest.fixture(name="mock_get_csv_data_from_s3", autouse=True)
def _get_csv_data_from_s3():
    with patch.object(s3_csv_etl,
                      "get_csv_data_from_s3") as mock:
        mock.return_value = TEST_DOWNLOADED_SHEET

        yield mock


@pytest.fixture(name="mock_create_or_extend_table_schema", autouse=True)
def _create_or_extend_table_schema():
    with patch.object(s3_csv_etl,
                      "create_or_extend_table_schema") as mock:
        yield mock


@pytest.fixture(name="mock_get_csv_dict_reader", autouse=True)
def _get_csv_dict_reader():
    with patch.object(s3_csv_etl,
                      "get_csv_dict_reader") as mock:
        yield mock


@pytest.fixture(name="mock_process_record_list", autouse=True)
def _process_record_list():
    with patch.object(s3_csv_etl,
                      "process_record_list") as mock:
        yield mock


@pytest.fixture(name="mock_load_file_into_bq", autouse=True)
def _load_file_into_bq():
    with patch.object(s3_csv_etl,
                      "load_file_into_bq") as mock:
        yield mock


@pytest.fixture(name="mock_merge_record_with_metadata")
def _merge_record_with_metadata():
    with patch.object(s3_csv_etl,
                      "merge_record_with_metadata") as mock:
        yield mock


@pytest.fixture(name="mock_get_initial_state", autouse=True)
def _get_initial_state():
    with patch.object(s3_csv_etl,
                      "get_initial_state") as mock:
        yield mock


@pytest.fixture(name="mock_download_s3_json_object", autouse=True)
def _download_s3_json_object():
    with patch.object(s3_csv_etl,
                      "download_s3_json_object") as mock:

        yield mock


@pytest.fixture(name="mock_write_to_file", autouse=True)
def _write_to_file():
    with patch.object(s3_csv_etl,
                      "write_jsonl_to_file") as mock:
        yield mock


class TestSheetRecordMetadata:
    @staticmethod
    def get_s3_csv_config(update_dict: Optional[dict] = None):
        original_csv_config_dict = {
            "dataPipelineId": "data-pipeline-id-1",
            "importedTimestampFieldName": "imported_timestamp",
            "objectKeyPattern": [
                "obj_key_pattern_1*",
                "obj_key_pattern_2*"
            ],
            "stateFile": {
                "bucketName": "{ENV}_bucket_name",
                "objectName": "{ENV}_object_prefix_1"
            }
        }
        csv_config_dict = {
            **original_csv_config_dict,
            **update_dict
        } if update_dict else {
            **original_csv_config_dict,
        }
        gcp_project = ""
        deployment_env = ""
        return S3BaseCsvConfig(
            csv_config_dict,
            gcp_project,
            deployment_env
        )

    test_timestamp_str = "2020-10-01T10:15:13Z"

    def test_contain_only_provenance_and_timestamp(self):

        config = TestSheetRecordMetadata.get_s3_csv_config()
        expected_record_metadata = {
            'imported_timestamp': TestSheetRecordMetadata.test_timestamp_str,
            'provenance': {'s3_bucket': '', 'source_filename': 's3_object'}
        }
        string_record_list = [
            "string rec 1",
            "string rec 2"
        ]
        s3_object = "s3_object"
        returned_metadata = get_record_metadata(
            string_record_list,
            config,
            s3_object,
            TestSheetRecordMetadata.test_timestamp_str
        )
        assert expected_record_metadata == returned_metadata

    def test_should_contain_all_values_in_a_list_element_on_a_line(self):
        in_sheet_metadata = {
            "inSheetRecordMetadata":
                [
                    {
                        "metadataSchemaFieldName": "metadata_example_1",
                        "metadataLineIndex": 0
                    },
                    {
                        "metadataSchemaFieldName": "metadata_example_2",
                        "metadataLineIndex": 1
                    }
                ]
        }

        config = TestSheetRecordMetadata.get_s3_csv_config(
            in_sheet_metadata
        )
        string_record_list = [
            "rec1 col1, rec1 col2, rec1 col3 ",
            "rec2 col1, rec2 col2, rec2 col3 ",
            "rec3 col1, rec3 col2, rec3 col3 ",
        ]

        expected_record_metadata = {
            'metadata_example_1': 'rec1 col1, rec1 col2, rec1 col3 ',
            'metadata_example_2': 'rec2 col1, rec2 col2, rec2 col3 ',
            'imported_timestamp': TestSheetRecordMetadata.test_timestamp_str,
            'provenance': {'s3_bucket': '', 'source_filename': 's3_object'}
        }

        s3_object = "s3_object"
        returned_metadata = get_record_metadata(
            string_record_list,
            config,
            s3_object,
            TestSheetRecordMetadata.test_timestamp_str
        )
        assert expected_record_metadata == returned_metadata

    def test_should_contain_static_values_from_config(self):
        fixed_metadata = {
            "fixedSheetRecordMetadata":
                [
                    {
                        "metadataSchemaFieldName": "fixed_sheet_field_name",
                        "fixedSheetValue": "fixed_sheet_value"
                    }
                ]
        }
        config = TestSheetRecordMetadata.get_s3_csv_config(
            fixed_metadata
        )
        expected_record_metadata = {
            'imported_timestamp': '2020-10-01T10:15:13Z',
            'fixed_sheet_field_name': 'fixed_sheet_value',
            'provenance': {'s3_bucket': '', 'source_filename': 's3_object'}
        }

        string_record_list = [
            "rec1 col1, rec1 col2, rec1 col3 ",
            "rec2 col1, rec2 col2, rec2 col3 ",
            "rec3 col1, rec3 col2, rec3 col3 ",
        ]
        s3_object = "s3_object"
        returned_metadata = get_record_metadata(
            string_record_list,
            config,
            s3_object,
            TestSheetRecordMetadata.test_timestamp_str
        )
        assert expected_record_metadata == returned_metadata

    def test_should_contain_static_values_in_metadata_and_values_in_csv_data(
            self
    ):
        sheet_metadata = {
            "inSheetRecordMetadata":
                [{"metadataSchemaFieldName": "metadata_example_1",
                  "metadataLineIndex": 1}],
            "fixedSheetRecordMetadata":
                [{"metadataSchemaFieldName": "fixed_sheet_field_name",
                  "fixedSheetValue": "fixed_sheet_value"}]
        }
        config = TestSheetRecordMetadata.get_s3_csv_config(
            sheet_metadata
        )

        string_record_list = [
            "rec1 col1, rec1 col2, rec1 col3 ",
            "rec2 col1, rec2 col2, rec2 col3 ",
            "rec3 col1, rec3 col2, rec3 col3 ",
        ]
        s3_object = "s3_object"
        returned_metadata = get_record_metadata(
            string_record_list,
            config,
            s3_object,
            TestSheetRecordMetadata.test_timestamp_str
        )

        expected_record_metadata = {
            'metadata_example_1': 'rec2 col1, rec2 col2, rec2 col3 ',
            'imported_timestamp': TestSheetRecordMetadata.test_timestamp_str,
            'fixed_sheet_field_name': 'fixed_sheet_value',
            'provenance': {'s3_bucket': '', 'source_filename': 's3_object'}}

        assert expected_record_metadata == returned_metadata


class TestCsvHeader:
    config_dict = {
        "dataPipelineId": "data-pipeline-id-1",
        "importedTimestampFieldName": "imported_timestamp",
        "headerLineIndex": 1,
        "datasetName": "{ENV}-dataset",
        "tableWriteAppend": "true",
        "objectKeyPattern": [
            "obj_key_pattern_1*",
            "obj_key_pattern_2*"
        ],
        "stateFile": {
            "bucketName": "{ENV}_bucket_name",
            "objectName": "{ENV}_object_prefix_1"
        }
    }
    gcp_project = ""
    deployment_env = ""
    s3_file_config = S3BaseCsvConfig(
        config_dict,
        gcp_project,
        deployment_env
    )
    string_record_list = [
        "random string",
        "First Name, Last_Name, Age%%, Univ., Country",
        "col1, col2, col3, col4, col5 ",
    ]

    def test_should_be_standardized(self):
        standardized_header_result = get_standardized_csv_header(
            TestCsvHeader.string_record_list,
            TestCsvHeader.s3_file_config
        )
        expected_result = [
            'first_name', 'last_name', 'age__', 'univ_', 'country'
        ]
        assert standardized_header_result == expected_result


class TestTransformAndLoadData:
    @staticmethod
    def get_csv_config(update_dict: Optional[dict] = None):
        if update_dict is None:
            update_dict = {}
        config_dict = {
            "dataPipelineId": "data-pipeline-id-1",
            "importedTimestampFieldName": "imported_timestamp",
            "headerLineIndex": 0,
            "datasetName": "{ENV}-dataset",
            "tableWriteAppend": "true",
            "objectKeyPattern": [
                "obj_key_pattern_1*",
                "obj_key_pattern_2*"
            ],
            "stateFile": {
                "bucketName": "{ENV}_bucket_name",
                "objectName": "{ENV}_object_prefix_1"
            }
        }

        config_dict.update(
            update_dict
        )
        gcp_project = "gcp_project"
        deployment_env = "dep_env"

        return S3BaseCsvConfig(
            config_dict,
            gcp_project,
            deployment_env
        )

    def test_should_transform_write_and_load_to_bq(
            self,
            mock_get_csv_dict_reader,
            mock_process_record_list,
            mock_load_file_into_bq
    ):
        record_import_timestamp_as_string = ""
        s3_object_name = "s3_object"
        transform_load_data(
            s3_object_name,
            TestTransformAndLoadData.get_csv_config(),
            record_import_timestamp_as_string,
        )
        mock_get_csv_dict_reader.assert_called()
        mock_process_record_list.assert_called()
        mock_load_file_into_bq.assert_called()


class TestProcessData:
    @staticmethod
    def get_csv_config(update_dict: Optional[dict] = None):
        if update_dict is None:
            update_dict = {}
        config_dict = {
            "dataPipelineId": "data-pipeline-id",
            "headerLineIndex": 0,
            "dataValuesStartLineIndex": 1,
            "datasetName": "{ENV}-dataset",
            "tableWriteAppend": "true",
            "objectKeyPattern": [
                "obj_key_pattern_1*",
                "obj_key_pattern_2*"
            ],
            "stateFile": {
                "bucketName": "{ENV}_bucket_name",
                "objectName": "{ENV}_object_prefix_1"
            }
        }

        config_dict.update(
            update_dict
        )
        gcp_project = "gcp_project"
        deployment_env = "dep_env"

        return S3BaseCsvConfig(
            config_dict,
            gcp_project,
            deployment_env
        )

    def test_should_process_record_values_using_list_of_function_references(
            self
    ):
        processing_func_config = {
            "recordProcessingSteps": ["html_unescape"]
        }
        csv_header = [
            'first_name', 'last_name', 'age', 'univ', 'country', 'html_encoded'
        ]
        csv_config = TestProcessData.get_csv_config(processing_func_config)
        csv_dictionary_reader = get_csv_dict_reader(
            TEST_DOWNLOADED_SHEET,
            csv_header,
            csv_config
        )
        processing_function_name_list = DEFAULT_PROCESSING_STEPS
        processing_function_name_list.extend(
            csv_config.record_processing_function_steps
        )

        record_metadata = {}
        processed_record = list(
            process_record_list(
                csv_dictionary_reader,
                record_metadata,
                processing_function_name_list
            )
        )
        expected_first_returned_record_value = {
            'first_name': 'Michael', 'last_name': 'Bonbi', 'age': '7',
            'univ': 'Univ of California',
            'country': 'United States', 'html_encoded': '£682m'
        }

        assert processed_record[0] == expected_first_returned_record_value

    def test_should_call_process_record_function_n_times(
            self, mock_merge_record_with_metadata
    ):
        standardized_csv_header = [
            'first_name', 'last_name', 'age', 'univ', 'country', 'html_encoded'
        ]
        csv_config = TestProcessData.get_csv_config()
        csv_dict_reader = get_csv_dict_reader(
            TEST_DOWNLOADED_SHEET,
            standardized_csv_header,
            csv_config
        )
        records_split = TEST_DOWNLOADED_SHEET.split("\n")

        records_length = len(records_split) - 2
        record_metadata = {}
        list(
            process_record_list(
                csv_dict_reader,
                record_metadata,
            )
        )
        assert mock_merge_record_with_metadata.call_count == records_length

    def test_should_get_first_data_line_as_dict(
            self
    ):
        csv_config = TestProcessData.get_csv_config()
        standardized_csv_header = [
            'first_name', 'last_name', 'age', 'univ', 'country', 'html_encoded'
        ]
        csv_dict_reader = get_csv_dict_reader(
            TEST_DOWNLOADED_SHEET,
            standardized_csv_header,
            csv_config
        )
        expected_value = OrderedDict(
            [
                ('first_name', "'Michael'"),
                ('last_name', " 'Bonbi'"),
                ('age', " '7'"),
                ('univ', "'Univ of California'"),
                ('country', " 'United States'"),
                ('html_encoded', "'&pound;682m'"),
                (None, [''])
            ]
        )
        assert next(csv_dict_reader) == expected_value


class TestStoredState:
    original_csv_config_dict = {
        "dataPipelineId": "data-pipeline-id-1",
        "importedTimestampFieldName": "imported_timestamp",
        "objectKeyPattern": [
            "obj_key_pattern_1*",
            "obj_key_pattern_2*"
        ],
        "stateFile": {
            "bucketName": "{ENV}_bucket_name",
            "objectName": "{ENV}_object_prefix_1"
        }
    }
    gcp_project = "gcp_project"
    deployment_env = "deployment_env"
    csv_config = S3BaseCsvConfig(
        original_csv_config_dict,
        gcp_project,
        deployment_env
    )

    def test_should_get_default_initial_state(self):
        returned_state = get_initial_state(
            TestStoredState.csv_config,
            DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE
        )
        expected_state = {
            'obj_key_pattern_1*': DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE,
            'obj_key_pattern_2*': DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE
        }

        assert returned_state == expected_state

    def test_should_get_initial_state(self):
        initial_time_as_string = "initial_time_as_string"
        returned_state = get_initial_state(
            TestStoredState.csv_config, initial_time_as_string
        )
        expected_state = {
            'obj_key_pattern_1*': initial_time_as_string,
            'obj_key_pattern_2*': initial_time_as_string
        }
        assert returned_state == expected_state

    def test_should_get_initial_state_for_csv_config_keys(
            self):
        returned_state = get_initial_state(
            TestStoredState.csv_config,
            DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE
        )

        assert set(
            TestStoredState.csv_config.s3_object_key_pattern_list
        ) == set(returned_state.keys())

    def test_should_get_default_state_on_no_such_s3_key_failure(
            self, mock_get_initial_state,
            mock_download_s3_json_object):

        error_response = {'Error': {'Code': 'NoSuchKey'}}
        side_effect = botocore.errorfactory.ClientError(
            error_response, 'unexpected')
        mock_download_s3_json_object.side_effect = side_effect

        get_stored_state(
            TestStoredState.csv_config,
            DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE
        )
        mock_get_initial_state.assert_called_with(
            TestStoredState.csv_config,
            DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE
        )

    def test_should_not_get_default_state_when_no_key_failure(
            self,
            mock_get_initial_state,
            mock_download_s3_json_object):
        mock_download_s3_json_object.return_value = {
            'obj_key_pattern_1*': DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE,
            'obj_key_pattern_2*': DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE
        }
        get_stored_state(
            TestStoredState.csv_config,
            DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE
        )
        mock_get_initial_state.assert_not_called()


class TestRecordWithMetadata:

    def test_should_merge_record_with_metadata(self):
        record = {"record_key": "record_value"}
        metadata = {"metadata_key": "metadata_value"}
        expected_value = {
            "record_key": "record_value",
            "metadata_key": "metadata_value"
        }
        assert merge_record_with_metadata(
            record, metadata) == expected_value

    def test_should_update_metadata_with_provenance(self):
        metadata = {"metadata_key": "metadata_value"}
        expected_value = {
            'metadata_key': 'metadata_value',
            'provenance': {
                's3_bucket': 's3_bucket', 'source_filename': 's3_object'
            }
        }
        s3_bucket = "s3_bucket"
        s3_object = "s3_object"
        assert update_metadata_with_provenance(
            metadata, s3_bucket, s3_object) == expected_value
