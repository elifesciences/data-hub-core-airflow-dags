from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from data_pipeline.twitter.etl_twitter import etl_get_user_tweets
from data_pipeline.twitter.twitter_config import (
    EtlType,
    get_filtered_pipeline_conf
)
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    get_default_args,
)

TWITTER_USER_TWEET_CONFIG_FILE_PATH_ENV_NAME = (
    "TWITTER_USER_TWEET_CONFIG_FILE_PATH"
)
TWITTER_USER_TWEET_PIPELINE_SCHEDULE_INTERVAL_ENV_NAME = (
    "TWITTER_USER_TWEET_PIPELINE_SCHEDULE_INTERVAL"
)

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"


# pylint: disable=unused-argument
def get_users_tweets(**kwargs):
    pipeline_conf_list = get_filtered_pipeline_conf(
        EtlType.Username, DEPLOYMENT_ENV_ENV_NAME,
        TWITTER_USER_TWEET_CONFIG_FILE_PATH_ENV_NAME
    )

    for pipeline_conf in pipeline_conf_list:
        etl_get_user_tweets(pipeline_conf,)


DAG_USERS_TWEETS_DATA_PIPELINE_ID = "Twitter_User_Tweets_Data_Pipeline"
with DAG(
        dag_id=DAG_USERS_TWEETS_DATA_PIPELINE_ID,
        default_args=get_default_args(),
        schedule_interval=None,
        dagrun_timeout=None,
) as tweet_mention_reply:
    PythonOperator(
        task_id='twitter_data_etl_tweets_mention_replies',
        python_callable=get_users_tweets,
        execution_timeout=None
    )
