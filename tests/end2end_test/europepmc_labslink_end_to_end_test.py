from dags.europepmc_labslink_data_export_pipeline import (
    DAG_ID
)

from tests.end2end_test import (
    enable_and_trigger_dag_and_wait_for_success
)
from tests.end2end_test.end_to_end_test_helper import (
    AirflowAPI
)


def test_dag_runs():
    airflow_api = AirflowAPI()
    enable_and_trigger_dag_and_wait_for_success(
        airflow_api=airflow_api,
        dag_id=DAG_ID
    )
