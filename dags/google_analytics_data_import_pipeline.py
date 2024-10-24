# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import logging
from datetime import timedelta
from typing import Sequence

import airflow
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


def create_ga_pipeline_dags() -> Sequence[airflow.DAG]:
    multi_ga_config = get_data_multi_config()
    airflow_config = multi_ga_config.default_airflow_config

    # run sequentially as one DAG for now
    with create_dag(
        dag_id=DAG_ID,
        dagrun_timeout=timedelta(days=1),
        **airflow_config.dag_parameters
    ) as dag:
        PythonOperator(
            task_id='etl_google_analytics',
            dag=dag,
            python_callable=google_analytics_etl,
            **airflow_config.task_parameters
        )

        return [dag]


DAGS = create_ga_pipeline_dags()

FIRST_DAG = DAGS[0]
