import os
import logging
import time

from dags.google_spreadsheet_pipeline_controller import (
    DAG_ID,
    TARGET_DAG_ID,
    SPREADSHEET_CONFIG_FILE_PATH_ENV_NAME,
)
from dags.google_spreadsheet_import_pipeline import (
    DEFAULT_DEPLOYMENT_ENV_VALUE, DEPLOYMENT_ENV_ENV_NAME
)
from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict
from data_pipeline.spreadsheet_data.google_spreadsheet_config import (
    MultiSpreadsheetConfig, MultiCsvSheet
)
from tests.end2end_test.end_to_end_test_helper import (
    AirflowAPI, simple_query
)

LOGGER = logging.getLogger(__name__)

AIRFLW_API = AirflowAPI()


# pylint: disable=broad-except
def test_dag_runs_data_imported():
    project, dataset, table = get_project_dataset_table()
    try:
        simple_query(
            query=TestQueryTemplate.CLEAN_TABLE_QUERY,
            project=project,
            dataset=dataset,
            table=table,
        )
    except Exception:
        LOGGER.info("table not cleaned, maybe it does not exist")

    AIRFLW_API.unpause_dag(TARGET_DAG_ID)
    execution_date = AIRFLW_API.trigger_dag(dag_id=DAG_ID)
    is_running = True
    while is_running:
        is_running = AIRFLW_API.is_dag_running(DAG_ID, execution_date)
        if not is_running:
            is_running = AIRFLW_API.is_triggered_dag_running(
                TARGET_DAG_ID
            )

        time.sleep(5)
        LOGGER.info("etl in progress")
    time.sleep(15)
    assert not is_running
    assert AIRFLW_API.get_dag_status(DAG_ID, execution_date) == "success"
    query_response = simple_query(
        query=TestQueryTemplate.READ_COUNT_TABLE_QUERY,
        project=project,
        dataset=dataset,
        table=table,
    )

    assert query_response[0].get("count") > 0


def get_project_dataset_table():
    conf_file_path = os.getenv(
        SPREADSHEET_CONFIG_FILE_PATH_ENV_NAME
    )
    data_config_dict = get_yaml_file_as_dict(conf_file_path)
    dep_env = os.getenv(
        DEPLOYMENT_ENV_ENV_NAME, DEFAULT_DEPLOYMENT_ENV_VALUE
    )
    multi_data_config = MultiSpreadsheetConfig(data_config_dict,)
    multi_sheet_config_dict_0 = list(
        multi_data_config.spreadsheets_config.values()
    )[0]

    multi_sheet_config_0 = MultiCsvSheet(
        multi_sheet_config_dict_0, dep_env
    )
    csv_config_0 = list(
        multi_sheet_config_0.sheets_config.values()
    )[0]

    return (
        csv_config_0.gcp_project,
        csv_config_0.dataset_name,
        csv_config_0.table_name
    )


# pylint: disable=too-few-public-methods, missing-class-docstring
class TestQueryTemplate:
    CLEAN_TABLE_QUERY = """
    Delete from `{project}.{dataset}.{table}` where true
    """
    READ_COUNT_TABLE_QUERY = """
    Select Count(*) AS count from `{project}.{dataset}.{table}`
    """
