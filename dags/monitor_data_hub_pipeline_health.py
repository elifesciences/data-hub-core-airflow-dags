# Note: DagBag.process_file skips files without "airflow" or "DAG" in them
import os
import logging

from datetime import timedelta

from data_pipeline.monitoring.cli import get_monitoring_config

from data_pipeline.utils.pipeline_config import (
    get_pipeline_config_for_env_name_and_config_parser
)

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)

from data_pipeline.monitoring.monitoring_config import (
    MonitoringConfig
)

from data_pipeline.monitoring.ping_healthchecks import main as ping

from data_pipeline.monitoring.data_hub_pipeline_health_check import (
    run_data_hub_pipeline_health_check
)

LOGGER = logging.getLogger(__name__)

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV = "ci"
MONITORING_CONFIG_FILE_PATH_ENV_NAME = "MONITORING_CONFIG_FILE_PATH"

DAG_ID = "Monitor_Data_Hub_Pipeline_Health"
MONITOR_DATA_HUB_PIPELINE_HEALTH_SCHEDULE_INTERVAL_ENV_NAME = (
    "MONITOR_DATA_HUB_PIPELINE_HEALTH_SCHEDULE_INTERVAL"
)

MONITOR_DATA_HUB_PIPELINE_HEALTH_DAG = create_dag(
    dag_id=DAG_ID,
    schedule=os.getenv(
        MONITOR_DATA_HUB_PIPELINE_HEALTH_SCHEDULE_INTERVAL_ENV_NAME, None
    ),
    dagrun_timeout=timedelta(days=1)
)


def data_config_from_xcom(context):
    dag_context = context["ti"]
    data_config_dict = dag_context.xcom_pull(
        key="data_config_dict", task_ids="get_data_config"
    )
    LOGGER.info('data_config_dict: %s', data_config_dict)
    data_config = get_pipeline_config_for_env_name_and_config_parser(
        MONITORING_CONFIG_FILE_PATH_ENV_NAME,
        MonitoringConfig.from_dict
    )
    LOGGER.info('data_config: %r', data_config)
    return data_config


def ping_health_checks_io(**__):
    ping()


def check_data_hub_tables_status(**_kwargs):
    logging.basicConfig(level='INFO')
    data_config = get_monitoring_config()

    run_data_hub_pipeline_health_check(
        project=data_config.project_name,
        dataset=data_config.dataset_name,
        table=data_config.table_name
    )


monitor_airflow_health_task = create_python_task(
    MONITOR_DATA_HUB_PIPELINE_HEALTH_DAG,
    "monitor_airflow_health",
    ping_health_checks_io,
    retries=5
)

check_data_hub_tables_status_task = create_python_task(
    MONITOR_DATA_HUB_PIPELINE_HEALTH_DAG,
    "check_data_hub_tables_status",
    check_data_hub_tables_status,
    retries=5
)

# pylint: disable=superfluous-parens
# defined dependencies between tasks in the DAG
_ = (check_data_hub_tables_status_task >> monitor_airflow_health_task)
