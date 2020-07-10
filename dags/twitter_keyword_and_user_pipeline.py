from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from data_pipeline.twitter.etl_twitter import etl_get_tweet
from data_pipeline.twitter.twitter_config import (
    TweetType, EtlType,
    get_filtered_pipeline_conf
)
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    get_default_args,
)

TWITTER_USER_AND_KEYWORD_CONFIG_FILE_PATH_ENV_NAME = (
    "TWITTER_USER_AND_KEYWORD_CONFIG_FILE_PATH"
)
TWITTER_USER_FOLLOWER_PIPELINE_SCHEDULE_INTERVAL_ENV_NAME = (
    "TWITTER_USER_AND_KEYWORD_PIPELINE_SCHEDULE_INTERVAL"
)

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"


def run_etl(etl_type: EtlType):
    pipeline_conf_list = get_filtered_pipeline_conf(
        etl_type, DEPLOYMENT_ENV_ENV_NAME,
        TWITTER_USER_AND_KEYWORD_CONFIG_FILE_PATH_ENV_NAME
    )
    tweet_component_to_search = [TweetType.From, TweetType.To, TweetType.Mention]
    for pipeline_conf in pipeline_conf_list:
        etl_get_tweet(pipeline_conf, tweet_component_to_search)


def get_users_tweets(**kwargs):
    run_etl(EtlType.Username)


def etl_search_keywords(**kwargs):
    run_etl(EtlType.Keyword)


DAG_USERS_TWEETS_DATA_PIPELINE_ID = "Twitter_User_Tweets_Replies_And_Mentions_Data_Pipeline"
with DAG(
    dag_id=DAG_USERS_TWEETS_DATA_PIPELINE_ID,
    default_args=get_default_args(),
    schedule_interval=None,
    dagrun_timeout=None,
) as tweet_mention_reply:
    [
        PythonOperator(
            task_id='twitter_data_etl_tweets_mention_replies',
            python_callable=get_users_tweets,
            execution_timeout=None
        ),
        PythonOperator(
            task_id='twitter_keyword_search_etl',
            python_callable=etl_search_keywords,
            execution_timeout=None
        )
    ]
