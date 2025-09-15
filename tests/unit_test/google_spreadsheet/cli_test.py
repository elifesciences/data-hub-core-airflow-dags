import json
from pathlib import Path
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

from data_pipeline.google_spreadsheet.google_spreadsheet_config_typing import (
    BaseGoogleSpreadsheetConfigDict,
    GoogleSpreadsheetConfigDict,
    MultiGoogleSpreadsheetConfigDict
)

from data_pipeline.google_spreadsheet.google_spreadsheet_config import MultiCsvSheet
from data_pipeline.utils.pipeline_config import get_deployment_env

import data_pipeline.google_spreadsheet.cli as cli_module
from data_pipeline.google_spreadsheet.cli import (
    GoogleSpreadsheetEnvironmentVariables,
    main
)


PIPELINE_ID_1 = 'pipeline_1'
SPREADSHEET_ID_1 = 'spreadsheet_1'

PIPELINE_CONFIG_DICT_1: BaseGoogleSpreadsheetConfigDict = {
    'dataPipelineId': PIPELINE_ID_1,
    'spreadsheetId': SPREADSHEET_ID_1,
    'sheets': [
        {
            'sheetName': 'Sheet1',
            'tableName': 'table_1',
            'datasetName': 'dataset_1',
            'sheetRange': 'A1:C10'
        }
    ]
}


MULTI_PIPELINE_CONFIG_DICT_1: MultiGoogleSpreadsheetConfigDict = {
    'gcpProjectName': 'test_project',
    'importedTimestampFieldName': 'imported_timestamp',
    'spreadsheets': [PIPELINE_CONFIG_DICT_1]
}


@pytest.fixture(name='etl_google_spreadsheet_mock', autouse=True)
def _etl_google_spreadsheet_mock() -> Iterator[MagicMock]:
    with patch.object(cli_module, 'etl_google_spreadsheet') as mock:
        yield mock


class TestMain:
    def test_should_call_etl_with_config(
        self,
        tmp_path: Path,
        mock_env: dict,
        etl_google_spreadsheet_mock: MagicMock
    ):
        config_path = tmp_path / 'config.yaml'
        mock_env[GoogleSpreadsheetEnvironmentVariables.CONFIG_FILE_PATH] = str(
            config_path
        )
        config_path.write_text(
            json.dumps(MULTI_PIPELINE_CONFIG_DICT_1),
            encoding='utf-8'
        )
        expected_config_dict: GoogleSpreadsheetConfigDict = {
            **PIPELINE_CONFIG_DICT_1,
            'gcpProjectName': MULTI_PIPELINE_CONFIG_DICT_1['gcpProjectName'],
            'importedTimestampFieldName': (
                MULTI_PIPELINE_CONFIG_DICT_1['importedTimestampFieldName']
            )
        }
        expected_config = MultiCsvSheet(
            expected_config_dict,
            deployment_env=get_deployment_env()
        )
        main(['--data-pipeline-id', PIPELINE_ID_1])
        etl_google_spreadsheet_mock.assert_called_once()
        args, _kwargs = etl_google_spreadsheet_mock.call_args
        assert args[0].spreadsheet_id == expected_config.spreadsheet_id
