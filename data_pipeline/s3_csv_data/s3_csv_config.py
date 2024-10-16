import hashlib
from typing import Optional

from data_pipeline.utils.csv.config import BaseCsvConfig
from data_pipeline.utils.pipeline_config import (
    AirflowConfig,
    update_deployment_env_placeholder
)


DEFAULT_INITIAL_S3_FILE_LAST_MODIFIED_DATE = "2019-04-11 21:10:13"


class MultiS3CsvConfig:
    def __init__(self,
                 multi_s3_csv_config: dict,
                 ):
        self.gcp_project = multi_s3_csv_config["gcpProjectName"]
        self.import_timestamp_field_name = multi_s3_csv_config["importedTimestampFieldName"]
        default_config_dict = multi_s3_csv_config.get('defaultConfig', {})
        self.default_airflow_config = AirflowConfig.from_optional_dict(
            default_config_dict.get('airflow')
        )
        self.s3_csv_config = [
            extend_s3_csv_config_with_state_file_info(
                extend_s3_csv_config_dict(
                    s3_csv,
                    self.gcp_project,
                    self.import_timestamp_field_name,
                ),
                multi_s3_csv_config["stateFile"]
            )
            for s3_csv in multi_s3_csv_config["s3Csv"]
        ]
        self.s3_csv_config_dict_by_pipeline_id = {
            s3_csv_config_dict['dataPipelineId']: s3_csv_config_dict
            for s3_csv_config_dict in self.s3_csv_config
        }


def extend_s3_csv_config_with_state_file_info(
        s3_csv_config_dict: dict,
        default_state_file_config: dict
):
    s3_state_file_info = s3_csv_config_dict.get("stateFile")
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
    return {
        **s3_csv_config_dict,
        "stateFile": s3_state_file_info
    }


def generate_hash(string_to_hash: str):
    hash_object = hashlib.sha1(string_to_hash.encode())
    return hash_object.hexdigest()


def get_s3_csv_etl_id(data_config_dict: dict):
    etl_dag_run_id = (
        "".join(data_config_dict.get("objectKeyPattern", []))
        + data_config_dict.get("bucketName", "")
    )
    return generate_hash(etl_dag_run_id)


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
class S3BaseCsvConfig(BaseCsvConfig):
    def __init__(
            self,
            csv_sheet_config: dict,
            deployment_env: Optional[str] = None,
            environment_placeholder: str = "{ENV}"
    ):
        updated_config = (
            update_deployment_env_placeholder(
                original_dict=csv_sheet_config,
                deployment_env=deployment_env,
                environment_placeholder=environment_placeholder
            )
            if deployment_env
            else csv_sheet_config
        )
        super().__init__(
            csv_sheet_config=updated_config,
        )
        self.s3_bucket_name = csv_sheet_config.get(
            "bucketName", ""
        )
        self.s3_object_key_pattern_list = updated_config.get(
            "objectKeyPattern", ""
        )
        self.etl_id = updated_config.get(
            "dataPipelineId",
            get_s3_csv_etl_id(csv_sheet_config)
        )
        self.state_file_bucket_name = updated_config.get(
            "stateFile", {}).get("bucketName")
        self.state_file_object_name = updated_config.get(
            "stateFile", {}).get("objectName")
        self.record_processing_function_steps = csv_sheet_config.get(
            "recordProcessingSteps", None
        )
