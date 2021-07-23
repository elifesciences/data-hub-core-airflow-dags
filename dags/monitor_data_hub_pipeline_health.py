# Note: DagBag.process_file skips files without "airflow" or "DAG" in them
import os
import logging

from datetime import timedelta

from data_pipeline.utils.pipeline_file_io import (
    get_yaml_file_as_dict
)

from data_pipeline.utils.pipeline_config import get_env_var_or_use_default

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
    schedule_interval=os.getenv(
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
    deployment_env_var = get_env_var_or_use_default(
        DEPLOYMENT_ENV_ENV_NAME, DEFAULT_DEPLOYMENT_ENV)
    data_config = MonitoringConfig(
        data_config_dict, deployment_env_var)
    LOGGER.info('data_config: %r', data_config)
    return data_config


def get_data_config(**kwargs):
    conf_file = get_env_var_or_use_default(
        MONITORING_CONFIG_FILE_PATH_ENV_NAME, ""
    )
    LOGGER.info('conf_file_path: %s', conf_file)
    data_config_dict = get_yaml_file_as_dict(conf_file)
    LOGGER.info('data_config_dict: %s', data_config_dict)
    kwargs["ti"].xcom_push(
        key="data_config_dict",
        value=data_config_dict
    )


def ping_health_checks_io(**__):
    ping()


def check_data_hub_tables_status(**kwargs):
    logging.basicConfig(level='INFO')
    data_config = data_config_from_xcom(kwargs)

    run_data_hub_pipeline_health_check(
        bucket_name=data_config.bucket_name,
        object_name=data_config.object_name,
        project=data_config.project_name,
        dataset=data_config.dataset_name,
        table=data_config.table_name
    )


get_data_config_task = create_python_task(
    MONITOR_DATA_HUB_PIPELINE_HEALTH_DAG,
    "get_data_config",
    get_data_config,
    retries=5
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

# defined dependencies between tasks in the DAG
_ = (
    get_data_config_task >> [
        monitor_airflow_health_task,
        check_data_hub_tables_status_task
    ]
)
