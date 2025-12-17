import logging

from data_pipeline.monitoring.monitoring_config import MonitoringConfig
from data_pipeline.monitoring.data_hub_pipeline_health_check import (
    run_data_hub_pipeline_health_check
)
from data_pipeline.monitoring.ping_healthchecks import ping
from data_pipeline.utils.pipeline_config import (
    get_pipeline_config_for_env_name_and_config_parser,
)

LOGGER = logging.getLogger(__name__)


class MonitoringEnvironmentVariables:
    CONFIG_FILE_PATH = 'MONITORING_CONFIG_FILE_PATH'


def get_monitoring_config() -> MonitoringConfig:
    return get_pipeline_config_for_env_name_and_config_parser(
        MonitoringEnvironmentVariables.CONFIG_FILE_PATH,
        MonitoringConfig.from_dict
    )


def monitoring_etl():
    data_config = get_monitoring_config()
    ping()
    run_data_hub_pipeline_health_check(
        project=data_config.project_name,
        dataset=data_config.dataset_name,
        table=data_config.table_name
    )
    LOGGER.info('Monitoring Config: %s', data_config)


def main():
    monitoring_etl()
    LOGGER.info('Monitoring pipeline completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
