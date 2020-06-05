# pylint: disable=too-many-instance-attributes,too-many-arguments,
class BaseCsvConfig:

    def __init__(
            self,
            csv_sheet_config: dict,
            gcp_project: str = None,
            imported_timestamp_field_name: str = None
    ):
        self.gcp_project = (
            gcp_project or
            csv_sheet_config.get("gcpProjectName")
        )
        self.import_timestamp_field_name = (
            imported_timestamp_field_name or
            csv_sheet_config.get(
                "importedTimestampFieldName"
            )
        )
        self.header_line_index = csv_sheet_config.get("headerLineIndex")
        self.data_values_start_line_index = csv_sheet_config.get(
            "dataValuesStartLineIndex"
        )
        self.table_name = csv_sheet_config.get(
            "tableName", ""
        )
        self.dataset_name = csv_sheet_config.get(
            "datasetName", ""
        )
        self.table_write_append_enabled = csv_sheet_config.get(
            "tableWriteAppend", False
        )
        self.fixed_sheet_record_metadata = {
            record.get("metadataSchemaFieldName"):
                record.get("fixedSheetValue")
            for record in csv_sheet_config.get("fixedSheetRecordMetadata", [])
        }
        self.in_sheet_record_metadata = {
            record.get("metadataSchemaFieldName"):
                record.get("metadataLineIndex")
            for record in csv_sheet_config.get("inSheetRecordMetadata", [])
        }
