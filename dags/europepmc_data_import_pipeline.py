# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import logging

from data_pipeline.europepmc.europepmc_config import EuropePmcConfig
from data_pipeline.europepmc.europepmc_pipeline import (
    fetch_article_data_from_europepmc_and_load_into_bigquery
)
from data_pipeline.utils.pipeline_config import (
    get_environment_variable_value,
    get_pipeline_config_for_env_name_and_config_parser
)

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)


class EuropePmcPipelineEnvironmentVariables:
    CONFIG_FILE_PATH = 'EUROPEPMC_CONFIG_FILE_PATH'
    SCHEDULE_INTERVAL = 'EUROPEPMC_PIPELINE_SCHEDULE_INTERVAL'


DAG_ID = 'EuropePmc_Pipeline'


LOGGER = logging.getLogger(__name__)


def get_pipeline_config() -> 'EuropePmcConfig':
    return get_pipeline_config_for_env_name_and_config_parser(
        EuropePmcPipelineEnvironmentVariables.CONFIG_FILE_PATH,
        EuropePmcConfig.from_dict
    )


def fetch_article_data_from_europepmc_and_load_into_bigquery_task(**_kwargs):
    fetch_article_data_from_europepmc_and_load_into_bigquery(
        get_pipeline_config()
    )


EUROPEPMC_DAG = create_dag(
    dag_id=DAG_ID,
    schedule_interval=get_environment_variable_value(
        EuropePmcPipelineEnvironmentVariables.SCHEDULE_INTERVAL,
        default_value=None
    )
)

create_python_task(
    EUROPEPMC_DAG,
    "fetch_article_data_from_europepmc_and_load_into_bigquery_task",
    fetch_article_data_from_europepmc_and_load_into_bigquery_task,
    retries=5
)
