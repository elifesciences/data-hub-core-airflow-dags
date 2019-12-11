class MultiSpreadsheetConfig:
    def __init__(self,
                 multi_spreadsheet_config: dict,
                 deployment_env: str = None
                 ):
        self.gcp_project = multi_spreadsheet_config.get("gcpProjectName")
        self.import_timestamp_field_name = multi_spreadsheet_config.get(
            "importedTimestampFieldName"
        )
        self.spreadsheets_config = {
            spreadsheet.get("spreadsheetId"): extend_spreadsheet_config_dict(
                spreadsheet,
                self.gcp_project,
                self.import_timestamp_field_name,
                deployment_env,
            )
            for spreadsheet in multi_spreadsheet_config.get("spreadsheets")
        }


def extend_spreadsheet_config_dict(
        spreadsheet_config_dict,
        gcp_project: str,
        imported_timestamp_field_name: str,
        deployment_env: str = None,
):
    spreadsheet_config_dict["gcpProjectName"] = gcp_project
    spreadsheet_config_dict[
        "importedTimestampFieldName"
    ] = imported_timestamp_field_name
    if deployment_env:
        spreadsheet_config_dict["datasetName"] = deployment_env

    return spreadsheet_config_dict


class MultiCsvSheet:
    def __init__(self, multi_sheet_config: dict):
        self.spreadsheet_id = multi_sheet_config.get("spreadsheetId")
        self.import_timestamp_field_name = multi_sheet_config.get(
            "importedTimestampFieldName"
        )
        self.gcp_project = multi_sheet_config.get("gcpProjectName")
        self.sheets_config = {
            sheet.get("sheetName"): CsvSheetConfig(
                sheet,
                self.spreadsheet_id,
                self.gcp_project,
                self.import_timestamp_field_name,
            )
            for sheet in multi_sheet_config.get("sheets")
        }

    def set_dataset_name(self, env):
        for _, sheet_config in self.sheets_config.items():
            sheet_config.set_dataset_name(new_dataset_name=env)


# pylint: disable=too-many-instance-attributes, simplifiable-if-expression
class CsvSheetConfig:
    def __init__(
            self,
            csv_sheet_config: dict,
            spreadsheet_id: str,
            gcp_project: str,
            imported_timestamp_field_name: str,
    ):
        self.gcp_project = gcp_project
        self.import_timestamp_field_name = imported_timestamp_field_name
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = csv_sheet_config.get("sheetName")
        self.sheet_range = csv_sheet_config.get("sheetRange")
        self.header_line_index = csv_sheet_config.get("headerLineIndex")
        self.data_values_start_line_index = csv_sheet_config.get(
            "dataValuesStartLineIndex"
        )
        self.table_name = csv_sheet_config.get("tableName")
        self.dataset_name = csv_sheet_config.get("datasetName")
        self.table_write_append = (
            True
            if csv_sheet_config.get("tableWriteAppend", "").lower() == "true"
            else False
        )
        self.metadata = {
            record.get("metadataSchemaFieldName"):
                record.get("metadataLineIndex")
            for record in csv_sheet_config.get("metadata", [])
        }
        self.fixed_sheet_metadata = {
            record.get("metadataSchemaFieldName"):
                record.get("fixedSheetValue")
            for record in csv_sheet_config.get("fixedSheetMetadata", [])
        }

    def set_dataset_name(self, new_dataset_name: str):
        self.dataset_name = new_dataset_name
