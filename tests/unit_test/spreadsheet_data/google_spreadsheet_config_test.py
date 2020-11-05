from data_pipeline.utils.pipeline_config import ConfigKeys
from data_pipeline.spreadsheet_data.google_spreadsheet_config import (
    get_sheet_config_id,
    MultiSpreadsheetConfig
)


class TestGetSheetConfigId:
    def test_should_use_id_from_config(self):
        assert get_sheet_config_id({
            ConfigKeys.DATA_PIPELINE_CONFIG_ID: '123'
        }, index=0) == '123'

    def test_should_use_single_output_table_from_config_and_index(self):
        assert get_sheet_config_id({
            'sheets': [{
                'tableName': 'table1'
            }]
        }, index=0) == 'table1_0'

    def test_should_use_multiple_output_table_from_config_and_index(self):
        assert get_sheet_config_id({
            'sheets': [{
                'tableName': 'table1'
            }, {
                'tableName': 'table2'
            }]
        }, index=0) == 'table1_table2_0'

    def test_should_fallback_to_index(self):
        assert get_sheet_config_id({'other': 'x'}, index=0) == '0'


class TestMultiSpreadsheetConfig:
    def test_should_keep_existing_id_of_web_config(self):
        multi_config = MultiSpreadsheetConfig({
            'spreadsheets': [{
                ConfigKeys.DATA_PIPELINE_CONFIG_ID: '123'
            }]
        })
        assert list(multi_config.spreadsheets_config.values())[0][
            ConfigKeys.DATA_PIPELINE_CONFIG_ID
        ] == '123'

    def test_should_add_id_to_web_config(self):
        multi_config = MultiSpreadsheetConfig({
            'spreadsheets': [{
                'sheets': [{'tableName': 'table1'}]
            }]
        })
        assert list(multi_config.spreadsheets_config.values())[0][
            ConfigKeys.DATA_PIPELINE_CONFIG_ID
        ] == 'table1_0'
