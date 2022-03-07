# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os
import logging

from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)


class EuropePmcPipelineEnvironmentVariables:
    CONFIG_FILE_PATH = "EUROPEPMC_CONFIG_FILE_PATH"


DAG_ID = "EuropePmc_Pipeline"


LOGGER = logging.getLogger(__name__)


def get_env_var_or_use_default(env_var_name, default_value=None):
    return os.getenv(env_var_name, default_value)


def get_pipeline_config():
    conf_file_path = os.getenv(
        EuropePmcPipelineEnvironmentVariables.CONFIG_FILE_PATH
    )
    pipeline_config_dict = get_yaml_file_as_dict(conf_file_path)
    LOGGER.info('pipeline_config_dict: %s', pipeline_config_dict)
    return pipeline_config_dict


def europepmc_dummy_task(**_kwargs):
    get_pipeline_config()


EUROPEPMC_DAG = create_dag(
    dag_id=DAG_ID,
    schedule_interval=None
)

dummy_task = create_python_task(
    EUROPEPMC_DAG,
    "europepmc_dummy_task",
    europepmc_dummy_task,
    retries=5
)
