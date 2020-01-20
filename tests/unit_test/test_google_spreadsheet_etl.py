from unittest.mock import patch
import pytest

from data_pipeline.spreadsheet_data import google_spreadsheet_etl
from data_pipeline.spreadsheet_data.google_spreadsheet_etl import (
    process_record_list,
    etl_google_spreadsheet
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
