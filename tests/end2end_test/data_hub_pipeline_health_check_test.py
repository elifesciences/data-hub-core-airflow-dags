from data_pipeline.monitoring.data_hub_pipeline_health_check import (
    run_data_hub_pipeline_health_check
)
from data_pipeline.monitoring.monitoring_config import MonitoringConfig
from data_pipeline.utils.pipeline_config import (
    get_pipeline_config_for_env_name_and_config_parser
)


MONITORING_CONFIG_FILE_PATH_ENV_NAME = "MONITORING_CONFIG_FILE_PATH"


def test_run_data_hub_pipeline_health_check():
    data_config = get_pipeline_config_for_env_name_and_config_parser(
        MONITORING_CONFIG_FILE_PATH_ENV_NAME,
        MonitoringConfig
    )
    run_data_hub_pipeline_health_check(
        project=data_config.project_name,
        dataset=data_config.dataset_name,
        table=data_config.table_name
    )
