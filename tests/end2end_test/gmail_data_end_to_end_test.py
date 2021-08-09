import logging
import time

from tests.end2end_test.end_to_end_test_helper import (
    AirflowAPI,
    simple_query
)


LOGGER = logging.getLogger(__name__)

AIRFLW_API = AirflowAPI()

DATASET = "ci"
TABLE = "gmail_thread_details"
PROJECT = "elife-data-pipeline"


# pylint: disable=broad-except
def test_dag_runs_data_imported():
    logging.basicConfig(level='INFO')
    try:
        simple_query(
            query=TestQueryTemplate.CLEAN_TABLE_QUERY,
            project=PROJECT,
            table=TABLE,
            dataset=DATASET,
        )
    except Exception:
        LOGGER.info("table not cleaned, maybe it does not exist")

    dag_id_gmail = "Gmail_Data_Import_Pipeline"
    AIRFLW_API.unpause_dag(dag_id_gmail)

    config = {
        "dataset": DATASET,
        "table": TABLE
    }
    execution_date = AIRFLW_API.trigger_dag(dag_id=dag_id_gmail, conf=config)
    is_running = True
    while is_running:
        is_running = AIRFLW_API.is_dag_running(dag_id_gmail, execution_date)
        time.sleep(5)
        LOGGER.info("etl in progress")
    assert not is_running
    assert AIRFLW_API.get_dag_status(dag_id_gmail, execution_date) == "success"

    query_response = simple_query(
        query=TestQueryTemplate.READ_COUNT_TABLE_QUERY,
        project=PROJECT,
        table=TABLE,
        dataset=DATASET,
    )
    assert query_response[0].get("count") > 0


# pylint: disable=too-few-public-methods, missing-class-docstring
class TestQueryTemplate:
    CLEAN_TABLE_QUERY = """
    Delete from `{project}.{dataset}.{table}` where true
    """
    READ_COUNT_TABLE_QUERY = """
    Select Count(*) AS count from `{project}.{dataset}.{table}`
    """
