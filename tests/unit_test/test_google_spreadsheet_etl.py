from unittest.mock import patch, call
import pytest

from data_pipeline.spreadsheet_data import google_spreadsheet_etl
from data_pipeline.spreadsheet_data.google_spreadsheet_etl import (
    process_record_list,
    etl_google_spreadsheet,
    update_metadata_with_provenance,
    process_record,
    transform_load_data,
    get_record_metadata,
    get_standardized_csv_header,
    get_sheet_range_from_config
)
from data_pipeline.spreadsheet_data.google_spreadsheet_config import (
    MultiCsvSheet, BaseCsvSheetConfig
)

# pylint: disable=unused-argument,too-many-arguments

TEST_DOWNLOADED_SHEET = [
    ['First Name', 'Last_Name', 'Age', 'Univ', 'Country'],
    ['Michael', 'Bonbi', '7',
     'University of California', 'United States'
     ],
    ['Robert', 'Alfonso', '', 'Univ of Cambridge', 'France'],
    ['Michael', 'Shayne', '', '', 'England'],
    ['Fred', 'Fredrick', '21', '', 'China']
]


@pytest.fixture(name="mock_get_table_schema_field_names")
def _get_table_schema_field_names():
    with patch.object(google_spreadsheet_etl,
                      "get_table_schema_field_names") as mock:
        yield mock


@pytest.fixture(name="mock_current_timestamp_as_string")
def _current_timestamp_as_string():
    with patch.object(google_spreadsheet_etl,
                      "get_current_timestamp_as_string") as mock:
        yield mock


@pytest.fixture(name="mock_temporary_file")
def _temporary_file():
    with patch.object(google_spreadsheet_etl,
                      "NamedTemporaryFile") as mock:
        yield mock


@pytest.fixture(name="mock_process_record")
def _process_record():
    with patch.object(google_spreadsheet_etl,
                      "process_record") as mock:
        yield mock


@pytest.fixture(name="mock_process_record_list", autouse=True)
def _process_record_list():
    with patch.object(google_spreadsheet_etl,
                      "process_record_list") as mock:
        yield mock


@pytest.fixture(name="mock_process_csv_sheet", autouse=True)
def _process_csv_sheet():
    with patch.object(google_spreadsheet_etl,
                      "process_csv_sheet") as mock:
        yield mock


@pytest.fixture(name="mock_get_standardized_csv_header", autouse=True)
def _get_standardized_csv_header():
    with patch.object(google_spreadsheet_etl,
                      "get_standardized_csv_header") as mock:
        yield mock


@pytest.fixture(name="mock_load_file_into_bq", autouse=True)
def _load_file_into_bq():
    with patch.object(google_spreadsheet_etl,
                      "load_file_into_bq") as mock:
        yield mock


@pytest.fixture(name="mock_does_bigquery_table_exist", autouse=True)
def _does_bigquery_table_exist():
    with patch.object(google_spreadsheet_etl,
                      "does_bigquery_table_exist") as mock:
        yield mock


@pytest.fixture(name="mock_write_to_file", autouse=True)
def _write_to_file():
    with patch.object(google_spreadsheet_etl,
                      "write_to_file") as mock:
        yield mock


@pytest.fixture(name="mock_download_google_spreadsheet_single_sheet",
                autouse=True)
def _download_google_spreadsheet_single_sheet():
    with patch.object(google_spreadsheet_etl,
                      "download_google_spreadsheet_single_sheet") as mock:
        mock.return_value = TEST_DOWNLOADED_SHEET
        yield mock


@pytest.fixture(name="mock_extend_nested_table_schema_if_new_fields_exist",
                autouse=True)
def _extend_nested_table_schema_if_new_fields_exist():
    with patch.object(
            google_spreadsheet_etl,
            "extend_nested_table_schema_if_new_fields_exist"
    ) as mock:
        yield mock


class TestRecordMetadata:
    @staticmethod
    def get_csv_config(update_dict: dict = None):

        csv_config_dict = {
            **TestRecordMetadata.csv_config_dict,
            **update_dict
        } if update_dict else {
            **TestRecordMetadata.csv_config_dict,
        }
        gcp_project = ""
        deployment_env = ""
        return BaseCsvSheetConfig(
            csv_config_dict,
            "spreadsheet_id", gcp_project,
            "imported_timestamp_field_name",
            deployment_env
        )

    csv_config_dict = {
        "sheetName": "sheet name-0",
        "datasetName": "{ENV}-dataset",
        "tableWriteAppend": "true"
    }

    test_timestamp_str = "2020-10-01T10:15:13Z"

    def test_contain_only_provenance_and_timestamp(self):

        config = TestRecordMetadata.get_csv_config()
        expected_record_metadata = {
            'imported_timestamp_field_name': '2020-10-01T10:15:13Z',
            'provenance': {
                'spreadsheet_id': 'spreadsheet_id',
                'sheet_name': 'sheet name-0'
            }
        }
        returned_metadata = get_record_metadata(
            TEST_DOWNLOADED_SHEET,
            config,
            TestRecordMetadata.test_timestamp_str
        )
        assert expected_record_metadata == returned_metadata

    def test_should_merge_all_values_in_a_list_element_on_a_line(self):
        in_sheet_metadata = {
            "inSheetRecordMetadata":
                [
                    {
                        "metadataSchemaFieldName": "metadata_example_01",
                        "metadataLineIndex": 0
                    },
                    {
                        "metadataSchemaFieldName": "metadata_example_02",
                        "metadataLineIndex": 1
                    }
                ]
        }

        config = TestRecordMetadata.get_csv_config(
            in_sheet_metadata
        )
        expected_record_metadata = {
            'metadata_example_01': 'First Name,Last_Name,Age,Univ,Country',
            'metadata_example_02':
                'Michael,Bonbi,7,University of California,United States',
            'imported_timestamp_field_name':
                '2020-10-01T10:15:13Z',
            'provenance': {
                'spreadsheet_id': 'spreadsheet_id',
                'sheet_name': 'sheet name-0'
            }
        }
        returned_metadata = get_record_metadata(
            TEST_DOWNLOADED_SHEET,
            config,
            TestRecordMetadata.test_timestamp_str
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
        config = TestRecordMetadata.get_csv_config(
            fixed_metadata
        )
        expected_record_metadata = {
            'imported_timestamp_field_name':
                '2020-10-01T10:15:13Z',
            'fixed_sheet_field_name':
                'fixed_sheet_value',
            'provenance': {
                'spreadsheet_id': 'spreadsheet_id',
                'sheet_name': 'sheet name-0'
            }
        }
        returned_metadata = get_record_metadata(
            TEST_DOWNLOADED_SHEET,
            config,
            TestRecordMetadata.test_timestamp_str
        )
        assert expected_record_metadata == returned_metadata

    def test_should_contain_static_values_in_metadata_and_values_in_csv_data(
            self
    ):
        sheet_metadata = {
            "inSheetRecordMetadata":
                [
                    {
                        "metadataSchemaFieldName": "metadata_example_1",
                        "metadataLineIndex": 1
                    }
                ],
            "fixedSheetRecordMetadata":
                [
                    {
                        "metadataSchemaFieldName": "fixed_sheet_field_name",
                        "fixedSheetValue": "fixed_sheet_value"
                    }
                ]
        }
        config = TestRecordMetadata.get_csv_config(
            sheet_metadata
        )
        expected_record_metadata = {
            'metadata_example_1':
                'Michael,Bonbi,7,University of California,United States',
            'imported_timestamp_field_name':
                '2020-10-01T10:15:13Z',
            'fixed_sheet_field_name': 'fixed_sheet_value',
            'provenance': {
                'spreadsheet_id': 'spreadsheet_id',
                'sheet_name': 'sheet name-0'
            }
        }
        returned_metadata = get_record_metadata(
            TEST_DOWNLOADED_SHEET,
            config,
            TestRecordMetadata.test_timestamp_str
        )
        assert expected_record_metadata == returned_metadata


class TestCsvHeader:
    sheet_config = BaseCsvSheetConfig(
        {
            "sheetName": "sheet name-0",
            "headerLineIndex": 0,
            "datasetName": "{ENV}-dataset",
            "tableWriteAppend": "true",
        }, "spreadsheet_id", "",
        "imported_timestamp_field_name", ""
    )

    def test_should_be_standardized(self):
        standardized_header_result = get_standardized_csv_header(
            ['First Name', 'Last_Name', 'Age%%', 'Univ.', 'Country']
        )
        expected_result = [
            'first_name', 'last_name', 'age__', 'univ_', 'country'
        ]
        assert standardized_header_result == expected_result

    def test_should_extract_from_line_specified_in_config(
            self,
            mock_does_bigquery_table_exist,
            mock_get_standardized_csv_header
    ):
        record_import_timestamp_as_string = ""
        full_temp_file_location = ""
        transform_load_data(
            TEST_DOWNLOADED_SHEET,
            TestCsvHeader.sheet_config,
            record_import_timestamp_as_string,
            full_temp_file_location
        )
        mock_does_bigquery_table_exist.assert_called()
        mock_get_standardized_csv_header.assert_called_with(
            TEST_DOWNLOADED_SHEET[TestCsvHeader.sheet_config.header_line_index]
        )


class TestTransformAndLoadData:
    @staticmethod
    def get_csv_config(update_dict: dict = None):
        if update_dict is None:
            update_dict = dict()
        config_dict = {
            "sheetName": "sheet name-0",
            "headerLineIndex": 0,
            "dataValuesStartLineIndex": 1,
            "datasetName": "{ENV}-dataset",
            "tableName": "table_name_1",
            "tableWriteAppend": "true",
        }
        if update_dict:
            config_dict.update(
                update_dict
            )
        gcp_project = ""
        deployment_env = ""
        return BaseCsvSheetConfig(
            config_dict,
            "spreadsheet_id", gcp_project,
            "imported_timestamp_field_name",
            deployment_env
        )

    def test_should_transform_write_and_load_to_bq(
            self,
            mock_load_file_into_bq, mock_does_bigquery_table_exist,
            mock_process_record_list, mock_write_to_file,
    ):
        record_import_timestamp_as_string = ""
        full_temp_file_location = ""
        transform_load_data(
            TEST_DOWNLOADED_SHEET,
            TestTransformAndLoadData.get_csv_config(),
            record_import_timestamp_as_string,
            full_temp_file_location
        )
        mock_does_bigquery_table_exist.assert_called()
        mock_process_record_list.assert_called()
        mock_write_to_file.assert_called()
        mock_load_file_into_bq.assert_called()

    def test_should_try_extend_table_if_table_does_exist(
            self,
            mock_load_file_into_bq, mock_does_bigquery_table_exist,
            mock_process_record_list, mock_write_to_file,
            mock_extend_nested_table_schema_if_new_fields_exist
    ):
        record_import_timestamp_as_string = ""
        full_temp_file_location = ""
        mock_does_bigquery_table_exist.return_value = True
        transform_load_data(
            TEST_DOWNLOADED_SHEET,
            TestTransformAndLoadData.get_csv_config(),
            record_import_timestamp_as_string,
            full_temp_file_location
        )
        mock_does_bigquery_table_exist.assert_called()
        (
            mock_extend_nested_table_schema_if_new_fields_exist.
            assert_called()
        )
        mock_process_record_list.assert_called()
        mock_write_to_file.assert_called()
        mock_load_file_into_bq.assert_called()


class TestProcessData:
    multi_csv_config_dict = {
        "spreadsheetId": "spreadsheet_id",
        "sheets": [
            {
                "sheetName": "sheet name-0",
                "datasetName": "{ENV}-dataset",
                "tableName": "table_name_1",
                "tableWriteAppend": "true",
            },
            {
                "sheetName": "sheet name-1",
                "datasetName": "{ENV}-dataset",
                "headerLineIndex": 0,
                "dataValuesStartLineIndex": 2,
                "tableName": "table_name_2",
                "tableWriteAppend": "false"
            }
        ]
    }

    sheet_config = BaseCsvSheetConfig(
        {
            "sheetName": "sheet name-0",
            "headerLineIndex": 0,
            "datasetName": "{ENV}-dataset",
            "tableWriteAppend": "true",
        }, "spreadsheet_id", "",
        "imported_timestamp_field_name", ""
    )

    def test_should_call_process_record_function_n_times(
            self, mock_process_record
    ):
        records_to_process = [["record_1"], ["record_2"]]
        records_length = len(records_to_process)
        records_header = ["header"]
        record_metadata = {}
        list(
            process_record_list(
                records_to_process,
                record_metadata,
                records_header
            )
        )
        assert mock_process_record.call_count == records_length

    def test_should_call_process_csv_sheet_function_n_times(
            self, mock_process_csv_sheet
    ):
        multi_csv_config = MultiCsvSheet(
            TestProcessData.multi_csv_config_dict, "dep_env"
        )
        spreadsheets_count = len(
            multi_csv_config.sheets_config.values()
        )
        etl_google_spreadsheet(multi_csv_config)
        assert mock_process_csv_sheet.call_count == spreadsheets_count

    def test_should_be_called_with_identical_timestamp(
            self,
            mock_process_csv_sheet,
            mock_current_timestamp_as_string,
            mock_temporary_file
    ):
        current_timestamp_as_string = "2010-11-11T 10:10:10Z"
        mock_current_timestamp_as_string.return_value = (
            current_timestamp_as_string
        )
        multi_csv_config = MultiCsvSheet(
            TestProcessData.multi_csv_config_dict, "dep_env"
        )
        temp_file = mock_temporary_file.return_value.__enter__()
        expected_calls = []
        for csv_sheet_config in multi_csv_config.sheets_config.values():
            process_call = call(
                csv_sheet_config,
                temp_file.name,
                current_timestamp_as_string
            )
            expected_calls.append(process_call)

        etl_google_spreadsheet(multi_csv_config)
        mock_process_csv_sheet.assert_has_calls(
            expected_calls
        )

        assert True


class TestRecord:
    def test_should_generated_json_from_record_extend_with_record_metadata(
            self
    ):
        record = ["record_val_1", "record_val_2",
                  "record_val_3", "record_val_4"]
        record_metadata = {
            "record_metadata_1": "_meta_1",
            "record_metadata_2": {}
        }
        standardized_csv_header = [
            "col_1", "col_2", "col_3", "col_4"
        ]
        expected_return = {
            'col_1': 'record_val_1',
            'col_2': 'record_val_2',
            'col_3': 'record_val_3',
            'col_4': 'record_val_4',
            'record_metadata_1': '_meta_1',
            'record_metadata_2': {}
        }

        actual_return = process_record(
            record,
            record_metadata,
            standardized_csv_header
        )
        assert expected_return == actual_return

    def test_should_update_record_metadata_with_provenance(self):
        test_config = BaseCsvSheetConfig(
            {
                "sheetName": "sheet name-0",
                "datasetName": "{ENV}-dataset",
                "tableName": "table_name_1",
                "tableWriteAppend": "true",
            }, "spreadsheet_id", "",
            "imported_timestamp_field_name", ""
        )
        expected_return = {
            'import_timestamp': '2019-01-01',
            'provenance': {
                'spreadsheet_id': 'spreadsheet_id',
                'sheet_name': 'sheet name-0'
            }
        }
        actual_return = update_metadata_with_provenance(
            {
                'import_timestamp': '2019-01-01'
            },
            test_config
        )
        assert expected_return == actual_return


class TestSpreadSheetSheetWithRange:
    def test_should_have_no_range(self):
        sheet_config = BaseCsvSheetConfig(
            {
                "sheetName": "sheet name-0",
                "headerLineIndex": 0,
                "datasetName": "{ENV}-dataset",
                "tableWriteAppend": "true",
            },
            "spreadsheet_id", "s_id",
            "imported_timestamp_field_name", ""
        )

        sheet_with_range = get_sheet_range_from_config(
            sheet_config
        )
        expected_sheet_with_range = "sheet name-0"
        assert sheet_with_range == (
            expected_sheet_with_range
        )

    def test_should_have_range(self):
        sheet_config = BaseCsvSheetConfig(
            {
                "sheetName": "sheet name-0",
                "sheetRange": "A:B",
                "headerLineIndex": 0,
                "datasetName": "{ENV}-dataset",
                "tableWriteAppend": "true",
            },
            "spreadsheet_id", "s_id",
            "imported_timestamp_field_name", ""
        )

        sheet_with_range = get_sheet_range_from_config(
            sheet_config
        )
        expected_sheet_with_range = "sheet name-0!A:B"
        assert sheet_with_range == (
            expected_sheet_with_range
        )
