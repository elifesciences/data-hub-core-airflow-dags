from dags.europepmc_data_import_pipeline import (
    DAG_ID,
    get_pipeline_config_list,
)

from tests.end2end_test import (
    trigger_run_test_pipeline,
    DataPipelineCloudResource
)
from tests.end2end_test.end_to_end_test_helper import (
    AirflowAPI
)


def get_data_pipeline_cloud_resource():
    config_list = get_pipeline_config_list()
    assert len(config_list) == 1
    config = config_list[0]
    return DataPipelineCloudResource(
        project_name=config.target.project_name,
        dataset_name=config.target.dataset_name,
        table_name=config.target.table_name,
        state_file_bucket_name=config.state.state_file.bucket_name,
        state_file_object_name=config.state.state_file.object_name
    )


def test_dag_runs_data_imported():
    airflow_api = AirflowAPI()
    data_pipeline_cloud_resource = get_data_pipeline_cloud_resource()
    trigger_run_test_pipeline(
        airflow_api=airflow_api,
        dag_id=DAG_ID,
        pipeline_cloud_resource=data_pipeline_cloud_resource
    )
