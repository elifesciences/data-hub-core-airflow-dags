# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import logging

from datetime import timedelta

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)

from monitoring.ping_healthchecks import ping

from monitoring.data_hub_pipeline_health_check import main

LOGGER = logging.getLogger(__name__)

DAG_ID = "Monitor_Data_Hub_Pipeline_Health"
# don't forget to add the env var to the formula - 3 hours!
MONITOR_DATA_HUB_PIPELINE_HEALTH_SCHEDULE_INTERVAL_ENV_NAME = (
    "MONITOR_DATA_HUB_PIPELINE_HEALTH_SCHEDULE_INTERVAL"
)

MONITOR_DATA_HUB_PIPELINE_HEALTH_DAG = create_dag(
    dag_id=DAG_ID,
    # schedule_interval=os.getenv(
    #     MONITOR_DATA_HUB_PIPELINE_HEALTH_SCHEDULE_INTERVAL_ENV_NAME
    # ),
    dagrun_timeout=timedelta(hours=6)
)


# pylint: disable=unused-argument
def ping_health_checks_io(**context):
    logging.basicConfig(level='INFO')
    ping()


def check_data_hub_tables_status(**context):
    logging.basicConfig(level='INFO')
    main()


monitor_airflow_health_task = create_python_task(
    MONITOR_DATA_HUB_PIPELINE_HEALTH_DAG,
    "monitor_airflow_health_task",
    ping_health_checks_io,
    retries=5
)

check_data_hub_tables_status_task = create_python_task(
    MONITOR_DATA_HUB_PIPELINE_HEALTH_DAG,
    "check_data_hub_tables_status_task",
    check_data_hub_tables_status,
    retries=5
)
