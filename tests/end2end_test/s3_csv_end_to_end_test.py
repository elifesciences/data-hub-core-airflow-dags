import os
import logging

from data_pipeline import get_yaml_file_as_dict
from data_pipeline.s3_csv_data.s3_csv_config import (
    S3BaseCsvConfig, MultiS3CsvConfig
)
from dags.s3_csv_import_controller import (
    DAG_ID,
    TARGET_DAG,
    S3_CSV_CONFIG_FILE_PATH_ENV_NAME,
)
from dags.s3_csv_import_pipeline import (
    DEFAULT_DEPLOYMENT_ENV_VALUE, DEPLOYMENT_ENV_ENV_NAME
)
from tests.end2end_test import (
    trigger_run_test_pipeline,
    DataPipelineCloudResource
)
from tests.end2end_test.end_to_end_test_helper import (
    AirflowAPI
)


LOGGER = logging.getLogger(__name__)


def test_dag_runs_data_imported():
    airflow_api = AirflowAPI()

    data_pipeline_cloud_resource = (
        get_data_pipeline_cloud_resource()
    )
    trigger_run_test_pipeline(
        airflow_api,
        DAG_ID,
        TARGET_DAG,
        data_pipeline_cloud_resource
    )


def get_data_pipeline_cloud_resource():
    conf_file_path = os.getenv(
        S3_CSV_CONFIG_FILE_PATH_ENV_NAME
    )
    data_config_dict = get_yaml_file_as_dict(conf_file_path)
    dep_env = os.getenv(
        DEPLOYMENT_ENV_ENV_NAME, DEFAULT_DEPLOYMENT_ENV_VALUE
    )
    multi_data_config = MultiS3CsvConfig(data_config_dict, )
    multi_s3_object_pattern_config_dict = list(
        multi_data_config.s3_csv_config
    )[0]

    multi_s3_object_pattern_config = S3BaseCsvConfig(
        multi_s3_object_pattern_config_dict, dep_env
    )

    return DataPipelineCloudResource(
        multi_s3_object_pattern_config.gcp_project,
        multi_s3_object_pattern_config.dataset_name,
        multi_s3_object_pattern_config.table_name,
        multi_s3_object_pattern_config.state_file_bucket_name,
        multi_s3_object_pattern_config.state_file_object_name
    )
