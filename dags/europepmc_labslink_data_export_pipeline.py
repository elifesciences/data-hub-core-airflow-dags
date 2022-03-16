# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os
import logging

from data_pipeline.europepmc.europepmc_labslink_config import (
    EuropePmcLabsLinkConfig
)
from data_pipeline.utils.pipeline_config import (
    get_deployment_env,
    get_environment_variable_value,
    update_deployment_env_placeholder
)

from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict

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
    deployment_env = get_deployment_env()
    LOGGER.info('deployment_env: %s', deployment_env)
    conf_file_path = os.getenv(
        EuropePmcLabsLinkPipelineEnvironmentVariables.CONFIG_FILE_PATH
    )
    pipeline_config_dict = update_deployment_env_placeholder(
        get_yaml_file_as_dict(conf_file_path),
        deployment_env=deployment_env
    )
    LOGGER.info('pipeline_config_dict: %s', pipeline_config_dict)
    pipeline_config = EuropePmcLabsLinkConfig.from_dict(pipeline_config_dict)
    LOGGER.info('pipeline_config: %s', pipeline_config)
    return pipeline_config


def dummy_task(**_kwargs):
    get_pipeline_config()


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
