# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os
import logging
from datetime import timedelta

from airflow.operators.python import PythonOperator

from data_pipeline.utils.pipeline_config import (
    get_pipeline_config_for_env_name_and_config_parser
)
from data_pipeline.google_analytics.ga_config import (
    ExternalTriggerConfig,
    MultiGoogleAnalyticsConfig,
    GoogleAnalyticsConfig
)
from data_pipeline.google_analytics.ga_pipeline import etl_google_analytics
from data_pipeline.google_analytics.ga_config import (
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


GOOGLE_ANALYTICS_DAG = create_dag(
    dag_id=DAG_ID,
    schedule_interval=os.getenv(
        GOOGLE_ANALYTICS_PIPELINE_SCHEDULE_INTERVAL_ENV_NAME
    ),
    dagrun_timeout=timedelta(minutes=60)
)


def get_data_multi_config() -> MultiGoogleAnalyticsConfig:
    return get_pipeline_config_for_env_name_and_config_parser(
        GOOGLE_ANALYTICS_CONFIG_FILE_PATH_ENV_NAME,
        MultiGoogleAnalyticsConfig
    )


def google_analytics_etl(**kwargs):
    multi_ga_config = get_data_multi_config()
    externally_triggered_parameters = kwargs['dag_run'].conf or {}

    externally_selected_start_date = parse_date_or_none(externally_triggered_parameters.get(
        ExternalTriggerConfig.START_DATE
    ))
    externally_selected_end_date = parse_date_or_none(externally_triggered_parameters.get(
        ExternalTriggerConfig.END_DATE
    ))

    for ga_config_dict in multi_ga_config.google_analytics_config:
        ga_config = GoogleAnalyticsConfig.from_dict(
            config=ga_config_dict,
            gcp_project=multi_ga_config.gcp_project,
            import_timestamp_field_name=(
                multi_ga_config.import_timestamp_field_name
            )
        )
        LOGGER.info('processing config: %r', ga_config)
        etl_google_analytics(
            ga_config=ga_config,
            externally_selected_start_date=externally_selected_start_date,
            externally_selected_end_date=externally_selected_end_date
        )


ETL_GA_TASK = PythonOperator(
    task_id='etl_google_analytics',
    dag=GOOGLE_ANALYTICS_DAG,
    python_callable=google_analytics_etl,
    retries=1
)
