import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from data_pipeline.twitter.data_from_twitter_rest_api import TwitterRestApi
from data_pipeline.twitter.etl_twitter import etl_get_users_retweets
from data_pipeline.twitter.twitter_config import (
    EtlType, get_filtered_pipeline_conf
)
from data_pipeline.twitter.twiter_auth import get_authorized_from_file
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    get_default_args,
)


TWITTER_USER_RETWEET_CONFIG_FILE_PATH_ENV_NAME = (
    "TWITTER_USER_RETWEET_CONFIG_FILE_PATH"
)
TWITTER_USER_RETWEET_PIPELINE_SCHEDULE_INTERVAL_ENV_NAME = (
    "TWITTER_USER_RETWEET_PIPELINE_SCHEDULE_INTERVAL"
)
TWITTER_AUTH_YAML_FILE_ENV_NAME = (
    'TWITTER_AUTH_YAML_FILE'
)

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"


# pylint: disable=unused-argument
def get_users_retweet(**kwargs):
    auth = get_authorized_from_file(
        os.getenv(TWITTER_AUTH_YAML_FILE_ENV_NAME)
    )
    tweepy_auth = TwitterRestApi(auth)
    pipeline_conf_list = get_filtered_pipeline_conf(
        EtlType.Retweets, DEPLOYMENT_ENV_ENV_NAME,
        TWITTER_USER_RETWEET_CONFIG_FILE_PATH_ENV_NAME
    )
    for pipeline_conf in pipeline_conf_list:
        etl_get_users_retweets(pipeline_conf, tweepy_auth)


DAG_USER_RETWEET_PIPELINE_ID = "Twitter_User_Retweet_Data_Pipeline"
with DAG(
        dag_id=DAG_USER_RETWEET_PIPELINE_ID,
        default_args=get_default_args(),
        schedule_interval=None,
        dagrun_timeout=None,
) as twitter_followers_dag:
    PythonOperator(
        task_id='twitter_data_etl_retweet',
        python_callable=get_users_retweet,
        execution_timeout=None
    )
