from unittest.mock import patch
import pytest

from dags import google_spreadsheet_pipeline_controller
from dags.google_spreadsheet_pipeline_controller import (
    trigger_spreadsheet_data_pipeline_dag, TARGET_DAG_ID
)


class TestData:
    TEST_DATA_MULTIPLE_SPREADSHEET = {
        "gcpProjectName": "test_proj",
        "importedTimestampFieldName": "imported_timestamp",
        "spreadsheets": [
            {
                "spreadsheetId": "spreadsheet_id-0",
                "sheets": [
                    {
                        "sheetName": "sheet name 1",
                    }
                ]
            },
            {
                "spreadsheetId": "spreadsheet_id-1",
                "sheets": [
                    {
                        "sheetName": "sheet name 1",
                    }
                ]
            }
        ]
    }
    TEST_DATA_SINGLE_SPREADSHEET = {
        "gcpProjectName": "test_proj",
        "importedTimestampFieldName": "imported_timestamp",
        "spreadsheets": [
            {
                "spreadsheetId": "spreadsheet_id-0",
                "sheets": [
                    {
                        "sheetName": "sheet name 1",
                    }
                ]
            }
        ]
    }

    def __init__(self):
        self.spreadsheet_count = len(
            TestData.TEST_DATA_MULTIPLE_SPREADSHEET.get(
                "spreadsheets"
            )
        )


@pytest.fixture(name="mock_simple_trigger_dag")
def _simple_trigger_dag():
    with patch.object(google_spreadsheet_pipeline_controller,
                      "simple_trigger_dag") as mock:
        yield mock


@pytest.fixture(name="mock_get_yaml_file_as_dict")
def _get_yaml_file_as_dict():
    with patch.object(google_spreadsheet_pipeline_controller,
                      "get_yaml_file_as_dict") as mock:
        yield mock


def test_should_call_trigger_dag_function_n_times(
        mock_simple_trigger_dag, mock_get_yaml_file_as_dict
):
    mock_get_yaml_file_as_dict.return_value = (
        TestData.TEST_DATA_MULTIPLE_SPREADSHEET
    )
    test_data = TestData()
    trigger_spreadsheet_data_pipeline_dag()
    assert mock_simple_trigger_dag.call_count == test_data.spreadsheet_count


def test_should_call_trigger_dag_function_with_parameter(
        mock_simple_trigger_dag, mock_get_yaml_file_as_dict
):
    mock_get_yaml_file_as_dict.return_value = (
        TestData.TEST_DATA_SINGLE_SPREADSHEET
    )
    single_spreadsheet_config = {
        **(
            TestData.TEST_DATA_SINGLE_SPREADSHEET.get(
                "spreadsheets"
            )[0]
        ),
        'gcpProjectName':
            TestData.TEST_DATA_SINGLE_SPREADSHEET.get(
                "gcpProjectName"
            ),
        'importedTimestampFieldName':
            TestData.TEST_DATA_SINGLE_SPREADSHEET.get(
                "importedTimestampFieldName"
            )
    }

    trigger_spreadsheet_data_pipeline_dag()
    mock_simple_trigger_dag.assert_called_with(
        dag_id=TARGET_DAG_ID, conf=single_spreadsheet_config
    )
