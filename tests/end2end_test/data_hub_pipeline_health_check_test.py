import logging
import os
from data_pipeline.monitoring.data_hub_pipeline_health_check import (
    run_data_hub_pipeline_health_check
)
from data_pipeline.monitoring.monitoring_config import MonitoringConfig
from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict


LOGGER = logging.getLogger(__name__)


MONITORING_CONFIG_FILE_PATH_ENV_NAME = "MONITORING_CONFIG_FILE_PATH"


def test_run_data_hub_pipeline_health_check():
    config_file_path = os.environ[
        MONITORING_CONFIG_FILE_PATH_ENV_NAME
    ]
    LOGGER.info('conf_file_path: %s', config_file_path)
    data_config_dict = get_yaml_file_as_dict(
        config_file_path
    )
    # Hardcoding to `staging` because we do not have `v_Data_Hub_Pipeline_Status`
    #   in the `ci` dataset
    deployment_env_var = 'staging'
    data_config = MonitoringConfig(
        data_config_dict,
        deployment_env_var
    )
    run_data_hub_pipeline_health_check(
        project=data_config.project_name,
        dataset=data_config.dataset_name,
        table=data_config.table_name
    )
