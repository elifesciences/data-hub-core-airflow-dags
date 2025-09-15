from data_pipeline.google_spreadsheet.google_spreadsheet_config import (
    MultiSpreadsheetConfig
)


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
