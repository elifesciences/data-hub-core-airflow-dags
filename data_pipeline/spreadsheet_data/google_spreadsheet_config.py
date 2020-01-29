class MultiSpreadsheetConfig:
    def __init__(self,
                 multi_spreadsheet_config: dict,
                 ):
        self.gcp_project = multi_spreadsheet_config.get("gcpProjectName")
        self.import_timestamp_field_name = multi_spreadsheet_config.get(
            "importedTimestampFieldName"
        )
        self.spreadsheets_config = {
            spreadsheet.get("spreadsheetId"): {
                **spreadsheet,
                "gcpProjectName": self.gcp_project,
                "importedTimestampFieldName": self.import_timestamp_field_name
            }
            for spreadsheet in multi_spreadsheet_config.get("spreadsheets")
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
    def __init__(self, multi_sheet_config: dict,
                 deployment_env: str,
                 ):
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
                deployment_env,
            )
            for sheet in multi_sheet_config.get("sheets")
        }


# pylint: disable=too-many-instance-attributes,too-many-arguments,
# pylint: disable=simplifiable-if-expression
class CsvSheetConfig:
    def __init__(
            self,
            csv_sheet_config: dict,
            spreadsheet_id: str,
            gcp_project: str,
            imported_timestamp_field_name: str,
            deployment_env: str,
            environment_placeholder: str = "{ENV}"
    ):
        self.gcp_project = gcp_project
        self.import_timestamp_field_name = imported_timestamp_field_name
        self.spreadsheet_id = spreadsheet_id
        self.header_line_index = csv_sheet_config.get("headerLineIndex")
        self.data_values_start_line_index = csv_sheet_config.get(
            "dataValuesStartLineIndex"
        )
        self.sheet_name = csv_sheet_config.get("sheetName")
        self.sheet_range = csv_sheet_config.get("sheetRange")
        self.table_name = csv_sheet_config.get("tableName")
        self.dataset_name = csv_sheet_config.get(
            "datasetName"
        ).replace(environment_placeholder, deployment_env)
        self.table_write_append_enabled = (
            csv_sheet_config.get("tableWriteAppend", False)
        )
        self.in_sheet_record_metadata = {
            record.get("metadataSchemaFieldName"):
                record.get("metadataLineIndex")
            for record in csv_sheet_config.get("inSheetRecordMetadata", [])
        }
        self.fixed_sheet_record_metadata = {
            record.get("metadataSchemaFieldName"):
                record.get("fixedSheetValue")
            for record in csv_sheet_config.get("fixedSheetRecordMetadata", [])
        }
