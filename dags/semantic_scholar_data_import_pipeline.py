# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import logging

from data_pipeline.semantic_scholar.semantic_scholar_config import SemanticScholarConfig
from data_pipeline.semantic_scholar.semantic_scholar_pipeline import (
    fetch_article_data_from_semantic_scholar_and_load_into_bigquery
)
from data_pipeline.utils.pipeline_config import (
    get_environment_variable_value,
    get_pipeline_config_for_env_name_and_config_parser
)

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)


class SemanticScholarPipelineEnvironmentVariables:
    CONFIG_FILE_PATH = 'SEMANTIC_SCHOLAR_CONFIG_FILE_PATH'
    SCHEDULE_INTERVAL = 'SEMANTIC_SCHOLAR_PIPELINE_SCHEDULE_INTERVAL'


DAG_ID = 'Semantic_Scholar_Data_Import_Pipeline'


LOGGER = logging.getLogger(__name__)


def get_pipeline_config() -> 'SemanticScholarConfig':
    return get_pipeline_config_for_env_name_and_config_parser(
        SemanticScholarPipelineEnvironmentVariables.CONFIG_FILE_PATH,
        SemanticScholarConfig.from_dict
    )


def fetch_article_data_from_semantic_scholar_and_load_into_bigquery_task(**_kwargs):
    fetch_article_data_from_semantic_scholar_and_load_into_bigquery(
        get_pipeline_config()
    )


SEMANTIC_SCHOLAR_DAG = create_dag(
    dag_id=DAG_ID,
    schedule=get_environment_variable_value(
        SemanticScholarPipelineEnvironmentVariables.SCHEDULE_INTERVAL,
        default_value=None
    ),
    max_active_tasks=1,
    max_active_runs=1
)

create_python_task(
    SEMANTIC_SCHOLAR_DAG,
    "fetch_article_data_from_semantic_scholar_and_load_into_bigquery_task",
    fetch_article_data_from_semantic_scholar_and_load_into_bigquery_task,
    retries=5
)
