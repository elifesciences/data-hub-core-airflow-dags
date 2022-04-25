from dags.semantic_scholar_data_import_pipeline import (
    DAG_ID,
    get_pipeline_config
)

from tests.end2end_test import (
    trigger_run_test_pipeline,
    DataPipelineCloudResource
)
from tests.end2end_test.end_to_end_test_helper import (
    AirflowAPI
)


def get_data_pipeline_cloud_resource():
    config = get_pipeline_config()

    return DataPipelineCloudResource(
        project_name=config.target.project_name,
        dataset_name=config.target.dataset_name,
        table_name=config.target.table_name
    )


def test_dag_runs_data_imported():
    airflow_api = AirflowAPI()
    data_pipeline_cloud_resource = get_data_pipeline_cloud_resource()
    trigger_run_test_pipeline(
        airflow_api=airflow_api,
        dag_id=DAG_ID,
        pipeline_cloud_resource=data_pipeline_cloud_resource
    )
