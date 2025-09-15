from typing import cast

from data_pipeline.google_spreadsheet.google_spreadsheet_config_typing import (
    GoogleSpreadsheetConfigDict,
    GoogleSpreadsheetSheetConfigDict,
    MultiGoogleSpreadsheetConfigDict
)
from data_pipeline.utils.csv.config import BaseCsvConfig
from data_pipeline.utils.pipeline_config import (
    update_deployment_env_placeholder
)


class MultiSpreadsheetConfig:
    def __init__(
        self,
        multi_spreadsheet_config: MultiGoogleSpreadsheetConfigDict
    ):
        self.gcp_project = multi_spreadsheet_config.get("gcpProjectName")
        self.import_timestamp_field_name = multi_spreadsheet_config.get(
            "importedTimestampFieldName"
        )
        self.spreadsheets_config = {
            spreadsheet.get("dataPipelineId"): {
                **spreadsheet,
                "gcpProjectName": self.gcp_project,
                "importedTimestampFieldName": self.import_timestamp_field_name
            }
            for index, spreadsheet in enumerate(multi_spreadsheet_config["spreadsheets"])
        }


def extend_spreadsheet_config_dict(
        spreadsheet_config_dict,
        gcp_project: str,
        imported_timestamp_field_name: str,
):
    spreadsheet_config_dict["gcpProjectName"] = gcp_project
    spreadsheet_config_dict[
        "importedTimestampFieldName"
    ] = imported_timestamp_field_name

    return spreadsheet_config_dict


class MultiCsvSheet:
    def __init__(
        self,
        multi_sheet_config: GoogleSpreadsheetConfigDict,
        deployment_env: str
    ):
        self.spreadsheet_id = multi_sheet_config["spreadsheetId"]
        self.import_timestamp_field_name = multi_sheet_config["importedTimestampFieldName"]
        self.gcp_project = multi_sheet_config["gcpProjectName"]
        self.sheets_config = {
            sheet["sheetName"]: BaseCsvSheetConfig(
                sheet,
                self.spreadsheet_id,
                self.gcp_project,
                self.import_timestamp_field_name,
                deployment_env,
            )
            for sheet in multi_sheet_config["sheets"]
        }


# pylint: disable=too-many-instance-attributes,too-many-arguments,
# pylint: disable=simplifiable-if-expression
class BaseCsvSheetConfig(BaseCsvConfig):
    def __init__(
        self,
        csv_sheet_config: GoogleSpreadsheetSheetConfigDict,
        spreadsheet_id: str,
        gcp_project: str,
        imported_timestamp_field_name: str,
        deployment_env: str,
        environment_placeholder: str = "{ENV}"
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

        self.sheet_name = csv_sheet_config["sheetName"]
        self.sheet_range = csv_sheet_config.get("sheetRange", '')
