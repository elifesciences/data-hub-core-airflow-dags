import logging
import time

from tests.end2end_test.end_to_end_test_helper import AirflowAPI, simple_query

LOGGER = logging.getLogger(__name__)

AIRFLW_API = AirflowAPI()
DATASET = "ci"
TABLE = "crossref_event"
PROJECT = "elife-data-pipeline"  # change  all to env variable


# Note that this test is disabled because its isn't guaranteed to work
# due to the erratic naature of the crossref event api
# pylint: disable=broad-except
def disabled_test_dag_runs_data_imported():
    try:
        simple_query(
            query=TestQueryTemplate.CLEAN_TABLE_QUERY,
            project=PROJECT,
            dataset=DATASET,
            table=TABLE,
        )
    except Exception:
        LOGGER.info("table not cleaned, maybe it does not exist")
    dag_id = "Load_Crossref_Event_Into_Bigquery"
    config = {
        "dataset": DATASET,
        "table": TABLE,
        "until_collected_date": "2019-10-01",
        "latest_download_date": {"10.7554": "2019-09-20"},
        "current_timestamp": "2012-10-01 00:00:00",
    }
    execution_date = AIRFLW_API.unpause_and_trigger_dag_and_return_execution_date(
        dag_id=dag_id,
        conf=config
    )
    is_running = True
    while is_running:
        is_running = AIRFLW_API.is_dag_running(dag_id, execution_date)
        time.sleep(5)
        LOGGER.info("etl in progress")
    assert not is_running
    assert AIRFLW_API.get_dag_status(dag_id, execution_date) == "success"

    query_response = simple_query(
        query=TestQueryTemplate.READ_COUNT_TABLE_QUERY,
        project=PROJECT,
        dataset=DATASET,
        table=TABLE,
    )
    assert query_response[0].get("count") > 0

    # clean up
    simple_query(
        query=TestQueryTemplate.CLEAN_TABLE_QUERY,
        project=PROJECT,
        dataset=DATASET,
        table=TABLE,
    )


# pylint: disable=too-few-public-methods, missing-class-docstring
class TestQueryTemplate:
    CLEAN_TABLE_QUERY = """
    Delete from `{project}.{dataset}.{table}` where true
    """
    READ_COUNT_TABLE_QUERY = """
    Select Count(*) AS count from `{project}.{dataset}.{table}`
    """
