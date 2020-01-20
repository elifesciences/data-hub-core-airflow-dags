from unittest.mock import patch
import pytest

from data_pipeline.spreadsheet_data import google_spreadsheet_etl
from data_pipeline.spreadsheet_data.google_spreadsheet_etl import (
    process_record_list,
    etl_google_spreadsheet,
    process_csv_sheet,
    get_sheet_range_from_config,
    current_timestamp_as_string,
    get_new_table_column_names,
    update_metadata_with_provenance,
    process_record,
)
from data_pipeline.spreadsheet_data.google_spreadsheet_config import (
    MultiCsvSheet
)


class TestData:
    TEST_DOWNLOADED_G_SPREADSHEET_DATA = [
        ['First Name', 'Last_Name', 'Age', 'Univ', 'Country'],
        ['michael', 'jackson', '7',
         'University of California', 'United States'
         ],
        ['Robert', 'De Niro', '', 'Univ of Cambridge', 'France'],
        ['Wayne', 'Rooney', '', '', 'England']
    ]
    TEST_SPREADSHEET_CONFIG_DATA = {
        "spreadsheetId": "spreadsheet_id",
        "sheets": [
            {
                "sheetName": "sheet name-0",
                "headerLineIndex": 0,
                "dataValuesStartLineIndex": 1,
                "datasetName": "{ENV}-dataset",
                "tableName": "table_name_1",
                "tableWriteAppend": "true",
                "metadata": [
                    {
                        "metadataSchemaFieldName": "metadata_example",
                        "metadataLineIndex": 0
                    }
                ]
            },
            {
                "sheetName": "sheet name-1",
                "sheetRange": "A1:B20",
                "headerLineIndex": 0,
                "dataValuesStartLineIndex": 2,
                "datasetName": "{ENV}",
                "tableName": "table_name_2",
                "tableWriteAppend": "false",
                "metadata": [
                    {
                        "metadataSchemaFieldName": "metadata_example_1",
                        "metadataLineIndex": 1
                    }
                ]
            }
        ]
    }

    def __init__(self):
        self.spreadsheet_count = 1
        self.multi_csv_config = MultiCsvSheet(
            TestData.TEST_SPREADSHEET_CONFIG_DATA, "dep_env"
        )
        self.sheet_1_config = list(
            self.multi_csv_config.sheets_config.values()
        )[0]
        self.sheet_2_config = list(
            self.multi_csv_config.sheets_config.values()
        )[1]
        self.sheet_1_sheet_range = 'sheet name-0'
        self.sheet_2_sheet_range = 'sheet name-1!A1:B20'


@pytest.fixture(name="mock_process_record")
def _process_record():
    with patch.object(google_spreadsheet_etl,
                      "process_record") as mock:
        yield mock


@pytest.fixture(name="mock_process_csv_sheet")
def _process_csv_sheet():
    with patch.object(google_spreadsheet_etl,
                      "process_csv_sheet") as mock:
        yield mock


@pytest.fixture(name="mock_transform_load_data")
def _transform_load_data():
    with patch.object(google_spreadsheet_etl,
                      "transform_load_data") as mock:
        yield mock


@pytest.fixture(name="mock_get_table_schema_field_names")
def _get_table_schema_field_names():
    with patch.object(google_spreadsheet_etl,
                      "get_table_schema_field_names") as mock:
        yield mock


@pytest.fixture(name="mock_load_file_into_bq")
def _load_file_into_bq():
    with patch.object(google_spreadsheet_etl,
                      "load_file_into_bq") as mock:
        yield mock


@pytest.fixture(name="mock_download_google_spreadsheet_single_sheet")
def _download_google_spreadsheet_single_sheet():
    with patch.object(google_spreadsheet_etl,
                      "download_google_spreadsheet_single_sheet") as mock:
        test_data = TestData()
        mock.return_value = test_data.TEST_DOWNLOADED_G_SPREADSHEET_DATA
        yield mock


def test_should_call_process_record_function_n_times(
        mock_process_record
):
    list(process_record_list([[], []], {}, []))
    assert mock_process_record.call_count == 2


def test_should_call_process_csv_sheet_function_n_times(
        mock_process_csv_sheet
):
    test_data = TestData()
    etl_google_spreadsheet(test_data.multi_csv_config)

    assert mock_process_csv_sheet.call_count == 2


def test_process_csv_sheet_should_call_transform_load_data(
        mock_download_google_spreadsheet_single_sheet,
        mock_transform_load_data
):
    test_data = TestData()
    process_csv_sheet(test_data.sheet_1_config, "temp_dir")

    mock_transform_load_data.assert_called()


def test_process_csv_sheet_should_call_download_google_spreadsheet_single_sheet(
        mock_download_google_spreadsheet_single_sheet,
        mock_transform_load_data
):
    test_data = TestData()
    temp_dir = "temp_dir"
    process_csv_sheet(test_data.sheet_1_config, temp_dir)
    mock_download_google_spreadsheet_single_sheet.assert_called()


def test_xxx(
):

    test_data = TestData()
    bb1 = get_sheet_range_from_config(test_data.sheet_1_config)
    bb2 = get_sheet_range_from_config(test_data.sheet_2_config)

    assert bb1 == test_data.sheet_1_sheet_range
    assert bb2 == test_data.sheet_2_sheet_range


def test_eexxx(
        mock_get_table_schema_field_names
):

    test_data = TestData()
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
        'col_4', 'metadata_col_1', 'col_1', 'metadata_col_2', 'col_2'
    ]
    expected_ret_value.sort()

    return_value = get_new_table_column_names(
        test_data.sheet_1_config,
        standardized_csv_header,
        record_metadata
    )
    return_value.sort()

    assert expected_ret_value == return_value


def test_eexexx():
    test_data = TestData()
    expected_return = {
        'a': 'z',
        'provenance': {
            'spreadsheet_id': 'spreadsheet_id', 'sheet_name': 'sheet name-0'
        }
    }
    actual_return = update_metadata_with_provenance(
        {
            "a": "z"
        },
        test_data.sheet_1_config
    )
    assert expected_return == actual_return


def test_oio():
    record = ["record_val_1", "record_val_2", "record_val_3", "record_val_4"]
    record_metadata = {
        "record_metadata_1": "_meta_1",
        "record_metadata_2": {}
    }
    standardized_csv_header = ["col_1", "col_2", "col_3", "col_4"]
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


