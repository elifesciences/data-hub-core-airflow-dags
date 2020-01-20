from unittest.mock import patch
import pytest

from dags import google_spreadsheet_pipeline_controller
from dags.google_spreadsheet_pipeline_controller import trigger_dag


class TestData:
    TEST_DATA = {
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

    def __init__(self):
        self.spreadsheet_count = len(TestData.TEST_DATA.get("spreadsheets"))


@pytest.fixture(name="mock_simple_trigger_dag")
def _simple_trigger_dag():
    with patch.object(google_spreadsheet_pipeline_controller,
                      "simple_trigger_dag") as mock:
        yield mock


@pytest.fixture(name="mock_get_yaml_file_as_dict")
def _get_yaml_file_as_dict():
    with patch.object(google_spreadsheet_pipeline_controller,
                      "get_yaml_file_as_dict") as mock:
        mock.return_value = TestData.TEST_DATA
        yield mock


# pylint: disable=unused-argument
def test_should_call_trigger_dag_function_n_times(
        mock_simple_trigger_dag, mock_get_yaml_file_as_dict
):
    test_data = TestData()
    trigger_dag()
    assert mock_simple_trigger_dag.call_count == test_data.spreadsheet_count
