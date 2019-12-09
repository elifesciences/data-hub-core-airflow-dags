class MultiCsvSheet:
    def __init__(self, multi_sheet_config: dict, import_timestamp_field_name: str = None):
        self.spreadsheet_id = multi_sheet_config.get("spreadsheetId")
        self.gcp_project = multi_sheet_config.get("gcpProjectName")
        self.sheets_config = {
            sheet.get("sheetName"): CsvSheetConfig(sheet)
            for sheet in
            multi_sheet_config.get("sheets")
        }
        self.import_timestamp_field_name = (import_timestamp_field_name or
                                            multi_sheet_config.get("importedTimestampFieldName")
                                            )

    def modify_config_based_on_deployment_env(self, env):
        for _, sheet_config in self.sheets_config.items():
            sheet_config.set_dataset_name(new_dataset_name=env)


class CsvSheetConfig:
    def __init__(self,  csv_sheet_config: dict, dataset_name: str = None):
        self.sheet_name = csv_sheet_config.get("sheetName")
        self.sheet_range = csv_sheet_config.get("sheetRange")
        self.header_line_index = csv_sheet_config.get("headerLineIndex")
        self.data_values_start_line_index = csv_sheet_config.get("dataValuesStartLineIndex")
        self.table_name = csv_sheet_config.get("tableName")
        self.dataset_name = dataset_name or csv_sheet_config.get("datasetName")
        self.header_mapping_to_schema = {
            record.get("dataSource"): record.get("dataDestination")
            for record in csv_sheet_config.get("headerMappingToSchema", [])
        }
        self.table_write_append = (
            True if csv_sheet_config.get("tableWriteAppend", "").lower() == "true"
            else False
        )
        self.metadata = {
            record.get("metadataSchemaFieldName"): record.get("metadataLineIndex")
            for record in csv_sheet_config.get("metadata", [])
        }
        self.fixed_sheet_metadata = {
            record.get("metadataSchemaFieldName"): record.get("fixedSheetValue")
            for record in csv_sheet_config.get("fixedSheetMetadata", [])
        }
        self.schema_bucket = csv_sheet_config.get("schema", {}).get("bucket")
        self.schema_bucket_object_name = csv_sheet_config.get("schema", {}).get("objectName")

    def set_dataset_name(self, new_dataset_name: str):
        self.dataset_name = new_dataset_name
