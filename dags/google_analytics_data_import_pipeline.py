# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os
import logging
from datetime import timedelta

from airflow.operators.python import PythonOperator

from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict
from data_pipeline.google_analytics.ga_config import (
    ExternalTriggerConfig,
    MultiGoogleAnalyticsConfig,
    GoogleAnalyticsConfig
)
from data_pipeline.google_analytics.ga_pipeline import etl_google_analytics
from data_pipeline.google_analytics.etl_state import (
    parse_date_or_none
)
from data_pipeline.utils.dags.data_pipeline_dag_utils import create_dag


LOGGER = logging.getLogger(__name__)
DAG_ID = "Google_Analytics_Data_Transfer"

GOOGLE_ANALYTICS_CONFIG_FILE_PATH_ENV_NAME = (
    "GOOGLE_ANALYTICS_CONFIG_FILE_PATH"
)
GOOGLE_ANALYTICS_PIPELINE_SCHEDULE_INTERVAL_ENV_NAME = (
    "GOOGLE_ANALYTICS_PIPELINE_SCHEDULE_INTERVAL"
)

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"


GOOGLE_ANALYTICS_DAG = create_dag(
    dag_id=DAG_ID,
    schedule_interval=os.getenv(
        GOOGLE_ANALYTICS_PIPELINE_SCHEDULE_INTERVAL_ENV_NAME
    ),
    dagrun_timeout=timedelta(minutes=60)
)


def get_data_config(**kwargs):
    conf_file_path = os.getenv(
        GOOGLE_ANALYTICS_CONFIG_FILE_PATH_ENV_NAME
    )
    data_config_dict = get_yaml_file_as_dict(
        conf_file_path
    )
    kwargs["ti"].xcom_push(
        key="multi_google_analytics_config_dict",
        value=data_config_dict
    )


def google_analytics_etl(**kwargs):
    externally_triggered_parameters = kwargs['dag_run'].conf or {}
    external_trigger_conf_dict = externally_triggered_parameters.get(
        ExternalTriggerConfig.GA_CONFIG
    )
    dag_context = kwargs["ti"]
    multi_google_analytics_config_dict = (
        external_trigger_conf_dict or
        dag_context.xcom_pull(
            key="multi_google_analytics_config_dict",
            task_ids="get_data_config"
        )
    )
    dep_env = (
        externally_triggered_parameters.get(
            ExternalTriggerConfig.DEPLOYMENT_ENV,
            os.getenv(
                DEPLOYMENT_ENV_ENV_NAME
            )
        )
    )

    externally_selected_start_date = parse_date_or_none(externally_triggered_parameters.get(
        ExternalTriggerConfig.START_DATE
    ))
    externally_selected_end_date = parse_date_or_none(externally_triggered_parameters.get(
        ExternalTriggerConfig.END_DATE
    ))

    multi_ga_config = MultiGoogleAnalyticsConfig(
        multi_google_analytics_config_dict,
        dep_env
    )

    for ga_config_dict in multi_ga_config.google_analytics_config:
        ga_config = GoogleAnalyticsConfig(
            config=ga_config_dict,
            gcp_project=multi_ga_config.gcp_project,
            import_timestamp_field_name=(
                multi_ga_config.import_timestamp_field_name
            )
        )
        etl_google_analytics(
            ga_config=ga_config,
            externally_selected_start_date=externally_selected_start_date,
            externally_selected_end_date=externally_selected_end_date
        )


GET_DATA_CONFIG_TASK = PythonOperator(
    task_id='get_data_config',
    dag=GOOGLE_ANALYTICS_DAG,
    python_callable=get_data_config,
    retries=5
)

ETL_GA_TASK = PythonOperator(
    task_id='etl_google_analytics',
    dag=GOOGLE_ANALYTICS_DAG,
    python_callable=google_analytics_etl,
    retries=1,
)

# pylint: disable=pointless-statement
ETL_GA_TASK << GET_DATA_CONFIG_TASK
