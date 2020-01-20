import hashlib


class MultiS3CsvConfig:
    def __init__(self,
                 multi_s3_csv_config: dict,
                 ):
        self.gcp_project = multi_s3_csv_config.get("gcpProjectName")
        self.import_timestamp_field_name = multi_s3_csv_config.get(
            "importedTimestampFieldName"
        )
        self.s3_csv_config = [
            extend_s3_csv_config_with_state_file_info(
                extend_s3_csv_config_dict(
                    extend_with_etl_id(s3_csv),
                    self.gcp_project,
                    self.import_timestamp_field_name,
                ),
                multi_s3_csv_config.get("stateFile")
            )
            for s3_csv in multi_s3_csv_config.get("s3Csv")
        ]


def extend_s3_csv_config_with_state_file_info(
        s3_csv_config_dict: dict,
        default_state_file_config: dict
):
    if not (
            s3_csv_config_dict.get("stateFile", {}).get("bucketName")
            and s3_csv_config_dict.get("stateFile", {}).get("objectName")
    ):
        s3_state_file_info = {
            "bucketName": default_state_file_config.get("defaultBucketName"),
            "objectName": default_state_file_config.get(
                "defaultSystemGeneratedObjectPrefix"
            ) + get_s3_csv_etl_id(s3_csv_config_dict) + ".json"
        }
        s3_csv_config_dict["stateFile"] = s3_state_file_info
    return s3_csv_config_dict


def generate_hash(string_to_hash: str):
    hash_object = hashlib.sha1(string_to_hash.encode())
    return hash_object.hexdigest()


def get_s3_csv_etl_id(data_config_dict: dict):
    etl_dag_run_id = (
        "".join(data_config_dict.get("objectKeyPattern", []))
        + data_config_dict.get("bucketName", "")
    )
    return generate_hash(etl_dag_run_id)


def extend_with_etl_id(s3_csv_config_dict: dict):
    s3_csv_config_dict["id"] = get_s3_csv_etl_id(s3_csv_config_dict)
    return s3_csv_config_dict


def extend_s3_csv_config_dict(
        s3_csv_config_dict,
        gcp_project: str,
        imported_timestamp_field_name: str,
):
    s3_csv_config_dict["gcpProjectName"] = gcp_project
    s3_csv_config_dict[
        "importedTimestampFieldName"
    ] = imported_timestamp_field_name

    return s3_csv_config_dict


# pylint: disable=too-many-instance-attributes,too-many-arguments,
# pylint: disable=simplifiable-if-expression
class S3CsvConfig:
    def __init__(
            self,
            csv_sheet_config: dict,
            deployment_env: str,
            environment_placeholder: str = "{ENV}"
    ):
        self.gcp_project = csv_sheet_config.get("gcpProjectName")
        self.import_timestamp_field_name = csv_sheet_config.get(
            "importedTimestampFieldName"
        )
        self.s3_bucket_name = csv_sheet_config.get("bucketName")
        self.s3_object_key_pattern_list = csv_sheet_config.get(
            "objectKeyPattern"
        )
        self.header_line_index = csv_sheet_config.get("headerLineIndex")
        self.data_values_start_line_index = csv_sheet_config.get(
            "dataValuesStartLineIndex"
        )
        self.table_name = csv_sheet_config.get("tableName")
        self.dataset_name = csv_sheet_config.get(
            "datasetName"
        ).replace(environment_placeholder, deployment_env)
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
        self.etl_id = csv_sheet_config.get("id")
        self.state_file_bucket_name = csv_sheet_config.get(
            "stateFile", {}
        ).get("bucketName")
        self.state_file_object_name = csv_sheet_config.get(
            "stateFile", {}
        ).get("objectName")
