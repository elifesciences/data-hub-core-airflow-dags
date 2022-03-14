# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os
import logging

from data_pipeline.europepmc.europepmc_config import EuropePmcConfig
from data_pipeline.europepmc.europepmc_pipeline import (
    fetch_article_data_from_europepmc_and_load_into_bigquery
)
from data_pipeline.utils.pipeline_config import (
    get_deployment_env,
    update_deployment_env_placeholder
)

from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)


class EuropePmcPipelineEnvironmentVariables:
    CONFIG_FILE_PATH = "EUROPEPMC_CONFIG_FILE_PATH"


DAG_ID = "EuropePmc_Pipeline"


LOGGER = logging.getLogger(__name__)


def get_pipeline_config() -> 'EuropePmcConfig':
    conf_file_path = os.getenv(
        EuropePmcPipelineEnvironmentVariables.CONFIG_FILE_PATH
    )
    pipeline_config_dict = get_yaml_file_as_dict(conf_file_path)
    deployment_env = get_deployment_env()
    LOGGER.info("deployment_env: %s", deployment_env)
    if deployment_env:
        pipeline_config_dict = update_deployment_env_placeholder(
            pipeline_config_dict,
            deployment_env,
            environment_placeholder="{ENV}"
        )
    LOGGER.info('pipeline_config_dict: %s', pipeline_config_dict)
    pipeline_config = EuropePmcConfig.from_dict(pipeline_config_dict)
    LOGGER.info('pipeline_config: %s', pipeline_config)
    return pipeline_config


def fetch_article_data_from_europepmc_and_load_into_bigquery_task(**_kwargs):
    fetch_article_data_from_europepmc_and_load_into_bigquery(
        get_pipeline_config()
    )


EUROPEPMC_DAG = create_dag(
    dag_id=DAG_ID,
    schedule_interval=None
)

create_python_task(
    EUROPEPMC_DAG,
    "fetch_article_data_from_europepmc_and_load_into_bigquery_task",
    fetch_article_data_from_europepmc_and_load_into_bigquery_task,
    retries=5
)
