import os
import logging
import time

from dags.s3_csv_import_controller import (
    DAG_ID,
    TARGET_DAG,
    S3_CSV_CONFIG_FILE_PATH_ENV_NAME,
    get_yaml_file_as_dict,
)

from dags.s3_csv_import_pipeline import (
    DEFAULT_DEPLOYMENT_ENV_VALUE, DEPLOYMENT_ENV_ENV_NAME
)
from tests.end2end_test.end_to_end_test_helper import (
    AirflowAPI, simple_query
)
from data_pipeline.s3_csv_data.s3_csv_config import (
    S3BaseCsvConfig, MultiS3CsvConfig
)
from data_pipeline.utils.data_store.s3_data_service import delete_s3_object

LOGGER = logging.getLogger(__name__)
AIRFLOW_API = AirflowAPI()


# pylint: disable=broad-except
def test_dag_runs_data_imported():
    (project_name,
     dataset_name,
     table_name,
     state_file_bucket_name,
     state_file_object_name
     ) = get_project_dataset_table()

    try:
        simple_query(
            query=TestQueryTemplate.CLEAN_TABLE_QUERY,
            project=project_name,
            dataset=dataset_name,
            table=table_name,
        )
    except Exception:
        LOGGER.info("table not cleaned, maybe it does not exist")
    try:
        delete_s3_object(state_file_bucket_name,
                         state_file_object_name
                         )
    except Exception:
        LOGGER.info("s3 object not deleted, may not exist")

    AIRFLOW_API.unpause_dag(TARGET_DAG)
    execution_date = AIRFLOW_API.trigger_dag(dag_id=DAG_ID)
    is_dag_running = True
    while is_dag_running:
        is_dag_running = AIRFLOW_API.is_dag_running(DAG_ID, execution_date)
        if not is_dag_running:
            time.sleep(15)
            is_dag_running = AIRFLOW_API.is_triggered_dag_running(
                TARGET_DAG
            )
        LOGGER.info("etl in progress")
        time.sleep(5)
    time.sleep(15)
    assert not is_dag_running
    assert AIRFLOW_API.get_dag_status(DAG_ID, execution_date) == "success"
    query_response = simple_query(
        query=TestQueryTemplate.READ_COUNT_TABLE_QUERY,
        project=project_name,
        dataset=dataset_name,
        table=table_name,
    )
    assert query_response[0].get("count") > 0


def get_project_dataset_table():
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

    return (
        multi_s3_object_pattern_config.gcp_project,
        multi_s3_object_pattern_config.dataset_name,
        multi_s3_object_pattern_config.table_name,
        multi_s3_object_pattern_config.state_file_bucket_name,
        multi_s3_object_pattern_config.state_file_object_name
    )


# pylint: disable=too-few-public-methods, missing-class-docstring
class TestQueryTemplate:
    CLEAN_TABLE_QUERY = """
    Delete from `{project}.{dataset}.{table}` where true
    """
    READ_COUNT_TABLE_QUERY = """
    Select Count(*) AS count from `{project}.{dataset}.{table}`
    """
