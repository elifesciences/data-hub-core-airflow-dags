# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import logging

from data_pipeline.europepmc.europepmc_labslink_config import (
    EuropePmcLabsLinkConfig
)
from data_pipeline.europepmc.europepmc_labslink_pipeline import (
    fetch_article_dois_from_bigquery_and_update_labslink_ftp
)
from data_pipeline.utils.pipeline_config import (
    get_environment_variable_value,
    get_pipeline_config_for_env_name_and_config_parser
)

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)


class EuropePmcLabsLinkPipelineEnvironmentVariables:
    CONFIG_FILE_PATH = 'EUROPEPMC_LABSLINK_CONFIG_FILE_PATH'
    SCHEDULE_INTERVAL = 'EUROPEPMC_LABSLNK_PIPELINE_SCHEDULE_INTERVAL'


DAG_ID = 'EuropePmc_LabsLink_Pipeline'


LOGGER = logging.getLogger(__name__)


def get_pipeline_config() -> 'EuropePmcLabsLinkConfig':
    return get_pipeline_config_for_env_name_and_config_parser(
        EuropePmcLabsLinkPipelineEnvironmentVariables.CONFIG_FILE_PATH,
        EuropePmcLabsLinkConfig.from_dict
    )


def dummy_task(**_kwargs):
    fetch_article_dois_from_bigquery_and_update_labslink_ftp(
        get_pipeline_config()
    )


EUROPEPMC_DAG = create_dag(
    dag_id=DAG_ID,
    schedule_interval=get_environment_variable_value(
        EuropePmcLabsLinkPipelineEnvironmentVariables.SCHEDULE_INTERVAL,
        default_value=None
    )
)

create_python_task(
    EUROPEPMC_DAG,
    "dummy_task",
    dummy_task,
    retries=5
)
