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


def update_deployment_env_placeholder(
        original_dict: dict,
        deployment_env: str,
        environment_placeholder: str,
):
    new_dict = dict()
    for key, val in original_dict.items():
        if isinstance(val, dict):
            tmp = update_deployment_env_placeholder(
                val,
                deployment_env,
                environment_placeholder
            )
            new_dict[key] = tmp
        elif isinstance(val, list):
            new_dict[key] = [
                replace_env_placeholder(
                    x,
                    deployment_env,
                    environment_placeholder
                )
                for x in val
            ]
        else:
            new_dict[key] = replace_env_placeholder(
                original_dict[key],
                deployment_env,
                environment_placeholder
            )
    return new_dict


def replace_env_placeholder(
        param_value,
        deployment_env: str,
        environment_placeholder: str
):
    new_value = param_value
    if isinstance(param_value, str):
        new_value = param_value.replace(
            environment_placeholder,
            deployment_env
        )
    return new_value
