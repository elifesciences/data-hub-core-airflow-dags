import logging

from dags.s3_csv_import_pipeline import (
    get_dag_id_for_s3_csv_config_dict,
    get_multi_csv_pipeline_config
)
from data_pipeline.s3_csv_data.s3_csv_config_typing import S3CsvConfigDict

from data_pipeline.s3_csv_data.s3_csv_config import (
    S3BaseCsvConfig
)
from tests.end2end_test import (
    trigger_run_test_pipeline,
    DataPipelineCloudResource
)
from tests.end2end_test.end_to_end_test_helper import (
    AirflowAPI
)

LOGGER = logging.getLogger(__name__)


def get_test_csv_pipeline_config_dict() -> S3CsvConfigDict:
    multi_data_config = get_multi_csv_pipeline_config()
    return multi_data_config.s3_csv_config[0]


def get_data_pipeline_cloud_resource(csv_config_dict: S3CsvConfigDict) -> DataPipelineCloudResource:
    single_s3_csv_config = S3BaseCsvConfig(csv_config_dict)

    return DataPipelineCloudResource(
        single_s3_csv_config.gcp_project,
        single_s3_csv_config.dataset_name,
        single_s3_csv_config.table_name,
        single_s3_csv_config.state_file_bucket_name,
        single_s3_csv_config.state_file_object_name
    )


def test_dag_runs_data_imported():
    single_csv_pipeline_config_dict = get_test_csv_pipeline_config_dict()
    airflow_api = AirflowAPI()
    data_pipeline_cloud_resource = (
        get_data_pipeline_cloud_resource(single_csv_pipeline_config_dict)
    )
    trigger_run_test_pipeline(
        airflow_api=airflow_api,
        dag_id=get_dag_id_for_s3_csv_config_dict(single_csv_pipeline_config_dict),
        pipeline_cloud_resource=data_pipeline_cloud_resource
    )
