# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import logging

from data_pipeline.twitter_ads_api.twitter_ads_api_config import (
    TwitterAdsApiConfig
)

from data_pipeline.twitter_ads_api.twitter_ads_api_pipeline import (
    fetch_twitter_ads_api_data_and_load_into_bq
)
from data_pipeline.utils.pipeline_config import (
    get_environment_variable_value,
    get_pipeline_config_for_env_name_and_config_parser
)

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)


class TwitterAdsApiEnvironmentVariables:
    CONFIG_FILE_PATH = 'TWITTER_ADS_API_CONFIG_FILE_PATH'
    SCHEDULE_INTERVAL = 'TWITTER_ADS_API_PIPELINE_SCHEDULE_INTERVAL'


DAG_ID = 'Twitter_Ads_Api_Pipeline'


LOGGER = logging.getLogger(__name__)


def get_pipeline_config() -> 'TwitterAdsApiConfig':
    return get_pipeline_config_for_env_name_and_config_parser(
        TwitterAdsApiEnvironmentVariables.CONFIG_FILE_PATH,
        TwitterAdsApiConfig.from_dict
    )


def fetch_twitter_ads_api_data_and_load_into_bq_task(**_kwargs):
    fetch_twitter_ads_api_data_and_load_into_bq(
        get_pipeline_config()
    )


TWITTER_ADS_API_DAG = create_dag(
    dag_id=DAG_ID,
    schedule_interval=get_environment_variable_value(
        TwitterAdsApiEnvironmentVariables.SCHEDULE_INTERVAL,
        default_value=None
    )
)

create_python_task(
    TWITTER_ADS_API_DAG,
    "fetch_twitter_ads_api_data_and_load_into_bq_task",
    fetch_twitter_ads_api_data_and_load_into_bq_task,
    retries=5
)
