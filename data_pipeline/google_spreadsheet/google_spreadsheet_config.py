from dataclasses import dataclass
from typing import Mapping, Optional, cast

from data_pipeline.google_spreadsheet.google_spreadsheet_config_typing import (
    GoogleSpreadsheetConfigDict,
    GoogleSpreadsheetSheetConfigDict,
    MultiGoogleSpreadsheetConfigDict
)
from data_pipeline.utils.csv.config import BaseCsvConfig
from data_pipeline.utils.pipeline_config import (
    StateFileConfig,
    update_deployment_env_placeholder
)


class MultiSpreadsheetConfig:
    def __init__(
        self,
        multi_spreadsheet_config: MultiGoogleSpreadsheetConfigDict
    ):
        self.gcp_project = multi_spreadsheet_config['gcpProjectName']
        self.import_timestamp_field_name = multi_spreadsheet_config[
            'importedTimestampFieldName'
        ]
        self.spreadsheets_config: Mapping[str, GoogleSpreadsheetConfigDict] = {
            spreadsheet['dataPipelineId']: {
                **spreadsheet,
                'gcpProjectName': self.gcp_project,
                'importedTimestampFieldName': self.import_timestamp_field_name
            }
            for index, spreadsheet in enumerate(multi_spreadsheet_config['spreadsheets'])
        }

    @staticmethod
    def from_dict(
        multi_spreadsheet_config: MultiGoogleSpreadsheetConfigDict
    ) -> 'MultiSpreadsheetConfig':
        return MultiSpreadsheetConfig(multi_spreadsheet_config)


class BaseCsvSheetConfig(BaseCsvConfig):
    def __init__(  # pylint: disable=too-many-arguments
        self,
        csv_sheet_config: GoogleSpreadsheetSheetConfigDict,
        spreadsheet_id: str,
        gcp_project: str,
        imported_timestamp_field_name: str,
        deployment_env: str,
        environment_placeholder: str = '{ENV}'
    ):
        updated_csv_sheet_config = update_deployment_env_placeholder(
            cast(dict, csv_sheet_config),
            deployment_env,
            environment_placeholder
        )
        super().__init__(
            csv_sheet_config=updated_csv_sheet_config,
            gcp_project=gcp_project,
            imported_timestamp_field_name=imported_timestamp_field_name
        )
        self.spreadsheet_id = spreadsheet_id

        self.sheet_name = csv_sheet_config['sheetName']
        self.sheet_range = csv_sheet_config.get('sheetRange', '')


@dataclass(frozen=True)
class MultiCsvSheetConfig:
    spreadsheet_id: str
    import_timestamp_field_name: str
    gcp_project: str
    sheets_config: Mapping[str, BaseCsvSheetConfig]
    state_file: Optional[StateFileConfig] = None

    @staticmethod
    def from_dict(
        multi_sheet_config: GoogleSpreadsheetConfigDict,
        deployment_env: str
    ) -> 'MultiCsvSheetConfig':
        spreadsheet_id = multi_sheet_config['spreadsheetId']
        import_timestamp_field_name = multi_sheet_config['importedTimestampFieldName']
        gcp_project = multi_sheet_config['gcpProjectName']
        return MultiCsvSheetConfig(
            spreadsheet_id=spreadsheet_id,
            import_timestamp_field_name=import_timestamp_field_name,
            gcp_project=gcp_project,
            sheets_config={
                sheet['sheetName']: BaseCsvSheetConfig(
                    csv_sheet_config=sheet,
                    spreadsheet_id=spreadsheet_id,
                    gcp_project=gcp_project,
                    imported_timestamp_field_name=import_timestamp_field_name,
                    deployment_env=deployment_env,
                )
                for sheet in multi_sheet_config['sheets']
            },
            state_file=StateFileConfig.from_optional_dict(
                multi_sheet_config.get('stateFile')
            )
        )
