from data_pipeline.google_spreadsheet.google_spreadsheet_config import (
    MultiCsvSheetConfig,
    MultiSpreadsheetConfig
)
from data_pipeline.google_spreadsheet.google_spreadsheet_config_typing import (
    GoogleSpreadsheetConfigDict,
    GoogleSpreadsheetSheetConfigDict
)
from data_pipeline.utils.pipeline_config import StateFileConfig
from data_pipeline.utils.pipeline_config_typing import StateFileConfigDict


GOOGLE_SPREADSHEET_SHEET_CONFIG_DICT_1: GoogleSpreadsheetSheetConfigDict = {
    'sheetName': 'Sheet1',
    'tableName': 'table_name_1',
    'datasetName': 'dataset_name_1'
}

GOOGLE_SPREADSHEET_CONFIG_DICT_1: GoogleSpreadsheetConfigDict = {
    'dataPipelineId': 'data_pipeline_1',
    'gcpProjectName': 'test-project',
    'importedTimestampFieldName': 'imported_timestamp',
    'spreadsheetId': 'spreadsheet-id-1',
    'sheets': [GOOGLE_SPREADSHEET_SHEET_CONFIG_DICT_1]
}

STATE_FILE_1: StateFileConfigDict = {
    'bucketName': '{ENV}-bucket',
    'objectName': '{ENV}/object'
}


class TestMultiCsvSheetConfig:
    def test_should_load_basic_config(self):
        config = MultiCsvSheetConfig.from_dict(
            GOOGLE_SPREADSHEET_CONFIG_DICT_1,
            deployment_env='dev'
        )
        assert config.spreadsheet_id == GOOGLE_SPREADSHEET_CONFIG_DICT_1['spreadsheetId']
        assert config.import_timestamp_field_name == (
            GOOGLE_SPREADSHEET_CONFIG_DICT_1['importedTimestampFieldName']
        )
        assert config.gcp_project == GOOGLE_SPREADSHEET_CONFIG_DICT_1['gcpProjectName']

    def test_should_load_sheets_config(self):
        config = MultiCsvSheetConfig.from_dict(
            {
                **GOOGLE_SPREADSHEET_CONFIG_DICT_1,
                'sheets': [GOOGLE_SPREADSHEET_SHEET_CONFIG_DICT_1]
            },
            deployment_env='dev'
        )
        sheet_name = GOOGLE_SPREADSHEET_SHEET_CONFIG_DICT_1['sheetName']
        sheet_config = config.sheets_config[sheet_name]
        assert sheet_config.gcp_project == config.gcp_project
        assert sheet_config.import_timestamp_field_name == config.import_timestamp_field_name
        assert sheet_config.spreadsheet_id == config.spreadsheet_id
        assert sheet_config.sheet_name == sheet_name
        assert sheet_config.table_name == GOOGLE_SPREADSHEET_SHEET_CONFIG_DICT_1['tableName']
        assert sheet_config.dataset_name == GOOGLE_SPREADSHEET_SHEET_CONFIG_DICT_1['datasetName']

    def test_should_not_fail_without_state_config(self):
        assert 'stateFile' not in GOOGLE_SPREADSHEET_CONFIG_DICT_1
        MultiCsvSheetConfig.from_dict(
            GOOGLE_SPREADSHEET_CONFIG_DICT_1,
            deployment_env='dev'
        )

    def test_should_load_state_config(self):
        config = MultiCsvSheetConfig.from_dict(
            {
                **GOOGLE_SPREADSHEET_CONFIG_DICT_1,
                'stateFile': STATE_FILE_1
            },
            deployment_env='dev'
        )
        assert config.state_file == StateFileConfig.from_dict(STATE_FILE_1)


class TestMultiSpreadsheetConfig:
    def test_should_keep_existing_id_of_web_config(self):
        multi_config = MultiSpreadsheetConfig.from_dict({
            'gcpProjectName': 'test-project',
            'importedTimestampFieldName': 'imported_timestamp',
            'spreadsheets': [{
                'dataPipelineId': '123'
            }]
        })
        assert list(multi_config.spreadsheets_config.values())[0][
            'dataPipelineId'
        ] == '123'
