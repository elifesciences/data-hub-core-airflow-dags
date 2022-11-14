# pylint: disable=too-many-instance-attributes,too-many-arguments,
from typing import Optional


class BaseCsvConfig:

    def __init__(
            self,
            csv_sheet_config: dict,
            gcp_project: Optional[str] = None,
            imported_timestamp_field_name: Optional[str] = None
    ):
        self.gcp_project = str((
            gcp_project or
            csv_sheet_config.get("gcpProjectName")
        ))
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
