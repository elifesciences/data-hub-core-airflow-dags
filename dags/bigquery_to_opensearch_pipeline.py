# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import logging
from typing import Sequence

from data_pipeline.opensearch.bigquery_to_opensearch_config import BigQueryToOpenSearchConfig
from data_pipeline.opensearch.bigquery_to_opensearch_pipeline import (
    fetch_documents_from_bigquery_and_load_into_opensearch_from_config_list
)
from data_pipeline.utils.pipeline_config import (
    get_environment_variable_value,
    get_pipeline_config_for_env_name_and_config_parser
)

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)


class BigQueryToOpenSearchPipelineEnvironmentVariables:
    CONFIG_FILE_PATH = 'BIGQUERY_TO_OPENSEARCH_CONFIG_FILE_PATH'
    SCHEDULE_INTERVAL = 'BIGQUERY_TO_OPENSEARCH_PIPELINE_SCHEDULE_INTERVAL'


DAG_ID = 'BigQuery_To_OpenSearch_Pipeline'


LOGGER = logging.getLogger(__name__)


def get_pipeline_config_list() -> Sequence[BigQueryToOpenSearchConfig]:
    return get_pipeline_config_for_env_name_and_config_parser(
        BigQueryToOpenSearchPipelineEnvironmentVariables.CONFIG_FILE_PATH,
        BigQueryToOpenSearchConfig.parse_config_list_from_dict
    )


def fetch_documents_from_bigquery_and_load_into_opensearch_task(**_kwargs):
    fetch_documents_from_bigquery_and_load_into_opensearch_from_config_list(
        get_pipeline_config_list()
    )


BIGQUERY_TO_OPENSEARCH_DAG = create_dag(
    dag_id=DAG_ID,
    schedule=get_environment_variable_value(
        BigQueryToOpenSearchPipelineEnvironmentVariables.SCHEDULE_INTERVAL,
        default_value=None
    )
)

create_python_task(
    BIGQUERY_TO_OPENSEARCH_DAG,
    "fetch_documents_from_bigquery_and_load_into_opensearch_task",
    fetch_documents_from_bigquery_and_load_into_opensearch_task,
    retries=5
)
