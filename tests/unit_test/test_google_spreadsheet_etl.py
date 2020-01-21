from unittest.mock import patch
import pytest

from data_pipeline.spreadsheet_data import google_spreadsheet_etl
from data_pipeline.spreadsheet_data.google_spreadsheet_etl import (
    process_record_list,
    etl_google_spreadsheet,
    get_new_table_column_names,
    update_metadata_with_provenance,
    process_record,
    transform_load_data,
    get_record_metadata,
    get_standardized_csv_header
)
from data_pipeline.spreadsheet_data.google_spreadsheet_config import (
    MultiCsvSheet, CsvSheetConfig
)

# pylint: disable=unused-argument,too-many-arguments

TEST_DOWNLOADED_SHEET = [
    ['First Name', 'Last_Name', 'Age', 'Univ', 'Country'],
    ['michael', 'jackson', '7',
     'University of California', 'United States'
     ],
    ['Robert', 'De Niro', '', 'Univ of Cambridge', 'France'],
    ['Wayne', 'Rooney', '', '', 'England'],
    ['Angela', 'Merkel', '21', '', 'China']
]


@pytest.fixture(name="mock_get_table_schema_field_names")
def _get_table_schema_field_names():
    with patch.object(google_spreadsheet_etl,
                      "get_table_schema_field_names") as mock:
        yield mock


@pytest.fixture(name="mock_process_record")
def _process_record():
    with patch.object(google_spreadsheet_etl,
                      "process_record") as mock:
        yield mock


@pytest.fixture(name="mock_process_record_list")
def _process_record_list():
    with patch.object(google_spreadsheet_etl,
                      "process_record_list") as mock:
        yield mock


@pytest.fixture(name="mock_process_csv_sheet")
def _process_csv_sheet():
    with patch.object(google_spreadsheet_etl,
                      "process_csv_sheet") as mock:
        yield mock


@pytest.fixture(name="mock_get_standardized_csv_header")
def _get_standardized_csv_header():
    with patch.object(google_spreadsheet_etl,
                      "get_standardized_csv_header") as mock:
        yield mock


@pytest.fixture(name="mock_load_file_into_bq")
def _load_file_into_bq():
    with patch.object(google_spreadsheet_etl,
                      "load_file_into_bq") as mock:
        yield mock


@pytest.fixture(name="mock_does_bigquery_table_exist")
def _does_bigquery_table_exist():
    with patch.object(google_spreadsheet_etl,
                      "does_bigquery_table_exist") as mock:
        yield mock


@pytest.fixture(name="mock_get_new_table_column_names")
def _get_new_table_column_names():
    with patch.object(google_spreadsheet_etl,
                      "get_new_table_column_names") as mock:
        yield mock


@pytest.fixture(name="mock_extend_table_schema_field_names")
def _extend_table_schema_field_names():
    with patch.object(google_spreadsheet_etl,
                      "extend_table_schema_field_names") as mock:
        yield mock


@pytest.fixture(name="mock_write_to_file")
def _write_to_file():
    with patch.object(google_spreadsheet_etl,
                      "write_to_file") as mock:
        yield mock


@pytest.fixture(name="mock_download_google_spreadsheet_single_sheet")
def _download_google_spreadsheet_single_sheet():
    with patch.object(google_spreadsheet_etl,
                      "download_google_spreadsheet_single_sheet") as mock:
        mock.return_value = TEST_DOWNLOADED_SHEET
        yield mock


class TestRecordMetadata:
    @staticmethod
    def get_csv_config(in_sheet_metadata=None, fixed_metadata=None):
        csv_config_dict = {
            **TestRecordMetadata.csv_config_dict,
            "metadata": in_sheet_metadata if in_sheet_metadata else [],
            "fixedSheetMetadata": fixed_metadata if fixed_metadata else []
        }
        return CsvSheetConfig(
            csv_config_dict,
            "spreadsheet_id",
            "", "imported_timestamp_field_name", ""
        )
    csv_config_dict = {
        "sheetName": "sheet name-0",
        "datasetName": "{ENV}-dataset",
        "tableWriteAppend": "true"
    }

    test_timestamp = "2020-10-01T10:15:13Z"

    def test_should_merge_all_values_in_a_list_element_on_a_line(self):
        in_sheet_metadata = [
            {
                "metadataSchemaFieldName": "metadata_example_01",
                "metadataLineIndex": 0
            },
            {
                "metadataSchemaFieldName": "metadata_example_02",
                "metadataLineIndex": 1
            }
        ]
        config = TestRecordMetadata.get_csv_config(
            in_sheet_metadata=in_sheet_metadata
        )
        expected_record_metadata = {
            'metadata_example_01': 'First Name,Last_Name,Age,Univ,Country',
            'metadata_example_02':
                'michael,jackson,7,University of California,United States',
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
            TestRecordMetadata.test_timestamp
        )
        assert expected_record_metadata == returned_metadata

    def test_should_contain_values_static_values_from_config(self):
        csv_config_dict = [
            {
                "metadataSchemaFieldName": "fixed_sheet_field_name",
                "fixedSheetValue": "fixed_sheet_value"
            }
        ]
        config = TestRecordMetadata.get_csv_config(
            fixed_metadata=csv_config_dict
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
            TestRecordMetadata.test_timestamp
        )
        assert expected_record_metadata == returned_metadata

    def test_should_be_empty(self):

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
            TestRecordMetadata.test_timestamp
        )
        assert expected_record_metadata == returned_metadata

    def test_should_contain_static_values_in_metadata_and_values_in_csv_data(
            self
    ):
        in_sheet_metadata = [
            {
                "metadataSchemaFieldName": "metadata_example_1",
                "metadataLineIndex": 1
            }
            ]
        fixed_sheet_metadata = [
            {
                "metadataSchemaFieldName": "fixed_sheet_field_name",
                "fixedSheetValue": "fixed_sheet_value"
            }
        ]
        config = TestRecordMetadata.get_csv_config(
            in_sheet_metadata, fixed_sheet_metadata
        )
        expected_record_metadata = {
            'metadata_example_1':
                'michael,jackson,7,University of California,United States',
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
            TestRecordMetadata.test_timestamp
        )
        assert expected_record_metadata == returned_metadata


class TestCsvHeader:
    sheet_config = CsvSheetConfig(
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
            mock_load_file_into_bq, mock_does_bigquery_table_exist,
            mock_get_new_table_column_names,
            mock_extend_table_schema_field_names,
            mock_process_record_list, mock_write_to_file,
            mock_get_standardized_csv_header
    ):
        transform_load_data(
            TEST_DOWNLOADED_SHEET,
            TestCsvHeader.sheet_config,
            "", ""
        )
        mock_does_bigquery_table_exist.assert_called()
        mock_get_standardized_csv_header.assert_called_with(
            TEST_DOWNLOADED_SHEET[TestCsvHeader.sheet_config.header_line_index]
        )


class TestTransformAndLoadData:
    @staticmethod
    def get_csv_config(key=None, value=None):
        config_dict = {
            "sheetName": "sheet name-0",
            "headerLineIndex": 0,
            "dataValuesStartLineIndex": 1,
            "datasetName": "{ENV}-dataset",
            "tableName": "table_name_1",
            "tableWriteAppend": "true",
        }
        config_dict.update(
            {key: value}
        )
        return CsvSheetConfig(
            config_dict,
            "spreadsheet_id", "",
            "imported_timestamp_field_name", ""
        )

    def test_should_not_extend_non_existing_table(
            self,
            mock_load_file_into_bq, mock_does_bigquery_table_exist,
            mock_get_new_table_column_names,
            mock_extend_table_schema_field_names,
            mock_process_record_list, mock_write_to_file,
    ):
        mock_does_bigquery_table_exist.return_value = False
        transform_load_data(
            TEST_DOWNLOADED_SHEET,
            TestTransformAndLoadData.get_csv_config(),
            "", ""
        )
        assert not mock_get_new_table_column_names.called
        assert not mock_extend_table_schema_field_names.called

    def test_should_only_extend_table_when_new_column_exist(
            self,
            mock_load_file_into_bq, mock_does_bigquery_table_exist,
            mock_get_new_table_column_names,
            mock_extend_table_schema_field_names,
            mock_process_record_list, mock_write_to_file,
    ):
        mock_does_bigquery_table_exist.return_value = True
        mock_get_new_table_column_names.return_value = ["new_column_1"]
        transform_load_data(
            TEST_DOWNLOADED_SHEET,
            TestTransformAndLoadData.get_csv_config(),
            "", ""
        )
        assert mock_extend_table_schema_field_names.called

    def test_should_transform_write_and_load_to_bq(
            self,
            mock_load_file_into_bq, mock_does_bigquery_table_exist,
            mock_get_new_table_column_names,
            mock_extend_table_schema_field_names,
            mock_process_record_list, mock_write_to_file,
    ):
        transform_load_data(
            TEST_DOWNLOADED_SHEET,
            TestTransformAndLoadData.get_csv_config(),
            "", ""
        )
        mock_does_bigquery_table_exist.assert_called()
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

    sheet_config = CsvSheetConfig(
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
        list(process_record_list([[], []], {}, []))
        assert mock_process_record.call_count == 2

    def test_should_call_process_csv_sheet_function_n_times(
            self, mock_process_csv_sheet
    ):
        multi_csv_config = MultiCsvSheet(
            TestProcessData.multi_csv_config_dict, "dep_env"
        )
        etl_google_spreadsheet(multi_csv_config)
        assert mock_process_csv_sheet.call_count == 2


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

    def test_should_update_record_metadata_with_provenance_(self):
        test_config = CsvSheetConfig(
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


class TestTableSchema:
    test_config = CsvSheetConfig(
        {
            "sheetName": "sheet name-0",
            "datasetName": "{ENV}-dataset",
            "tableName": "table_name_1",
            "tableWriteAppend": "true",
            "metadata": [
                {
                    "metadataSchemaFieldName": "metadata_example",
                    "metadataLineIndex": 0
                }
            ]
        }, "spreadsheet_id", "",
        "imported_timestamp_field_name", ""
    )

    def test_should_get_new_cols_from_csv_header(
            self, mock_get_table_schema_field_names
    ):
        mock_get_table_schema_field_names.return_value = [
            "col_1", "col_2", "col_3", "metadata_col_1"
        ]
        standardized_csv_header = [
            "col_1", "col_2", "col_4"
        ]
        record_metadata = {
            "metadata_col_1": 0,
            "metadata_col_2": 0
        }
        expected_ret_value = ['col_4', 'metadata_col_2']
        expected_ret_value.sort()

        return_value = get_new_table_column_names(
            TestTableSchema.test_config,
            standardized_csv_header,
            record_metadata
        )
        return_value.sort()
        assert expected_ret_value == return_value

    def test_should_get_new_cols_from_csv_header_and_metadata(
            self, mock_get_table_schema_field_names
    ):
        mock_get_table_schema_field_names.return_values = [
            "col_1", "col_2", "col_3", "metadata_col_1"
        ]
        standardized_csv_header = [
            "col_1", "col_2", "col_4"
        ]
        record_metadata = {
            "metadata_col_1": 0,
            "metadata_col_2": 0
        }
        expected_ret_value = [
            'col_4', 'metadata_col_1', 'col_1',
            'metadata_col_2', 'col_2'
        ]
        expected_ret_value.sort()

        return_value = get_new_table_column_names(
            TestTableSchema.test_config,
            standardized_csv_header,
            record_metadata
        )
        return_value.sort()

        assert expected_ret_value == return_value
