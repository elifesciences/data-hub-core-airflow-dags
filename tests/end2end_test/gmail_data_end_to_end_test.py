import os

from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict
from data_pipeline.gmail_data.get_gmail_data_config import (
    MultiGmailDataConfig, GmailDataConfig
)
from dags.gmail_data_import_controller import (
    DAG_ID,
    TARGET_DAG_ID,
    GMAIL_DATA_CONFIG_FILE_PATH_ENV_NAME
)
from dags.gmail_data_import_pipeline import (
    DEPLOYMENT_ENV_ENV_NAME, DEFAULT_DEPLOYMENT_ENV
)

from tests.end2end_test import (
    trigger_run_test_pipeline,
    DataPipelineCloudResource
)
from tests.end2end_test.end_to_end_test_helper import (
    AirflowAPI
)


def test_dag_runs_data_imported():
    airflow_api = AirflowAPI()
    data_pipeline_cloud_resource = (
        get_data_pipeline_cloud_resource()
    )
    trigger_run_test_pipeline(
        airflow_api=airflow_api,
        dag_id=DAG_ID,
        target_dag=TARGET_DAG_ID,
        pipeline_cloud_resource=data_pipeline_cloud_resource
    )


def get_data_pipeline_cloud_resource():
    conf_file_path = os.getenv(
        GMAIL_DATA_CONFIG_FILE_PATH_ENV_NAME
    )
    data_config_dict = get_yaml_file_as_dict(conf_file_path)
    dep_env = os.getenv(
        DEPLOYMENT_ENV_ENV_NAME, DEFAULT_DEPLOYMENT_ENV
    )
    multi_data_config = MultiGmailDataConfig(data_config_dict)
    single_gmail_config_dict = list(
        multi_data_config.gmail_data_config.values()
    )[0]
    single_gmail_config = GmailDataConfig(
        data_config=single_gmail_config_dict,
        deployment_env=dep_env
    )

    return DataPipelineCloudResource(
        project_name=single_gmail_config.project_name,
        dataset_name=single_gmail_config.dataset_name,
        table_name=single_gmail_config.table_name_thread_ids,
        state_file_bucket_name=None,
        state_file_object_name=None
    )
