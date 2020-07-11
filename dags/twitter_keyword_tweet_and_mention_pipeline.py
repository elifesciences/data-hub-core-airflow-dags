from typing import List

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from data_pipeline.twitter.etl_twitter import etl_get_tweets
from data_pipeline.twitter.twitter_config import (
    TweetType, EtlType,
    get_filtered_pipeline_conf
)
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    get_default_args,
)

TWITTER_KEYWORD_MENTIONS_REPLIES_CONFIG_FILE_PATH_ENV_NAME = (
    "TWITTER_KEYWORD_MENTIONS_REPLIES_CONFIG_FILE_PATH"
)
TWITTER_KEYWORD_MENTIONS_REPLIES_SCHEDULE_INTERVAL_ENV_NAME = (
    "TWITTER_KEYWORD_MENTIONS_REPLIES_PIPELINE_SCHEDULE_INTERVAL"
)

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"


def run_etl(etl_type: EtlType, tweet_component_to_search: List[TweetType]):
    pipeline_conf_list = get_filtered_pipeline_conf(
        etl_type, DEPLOYMENT_ENV_ENV_NAME,
        TWITTER_KEYWORD_MENTIONS_REPLIES_CONFIG_FILE_PATH_ENV_NAME
    )
    for pipeline_conf in pipeline_conf_list:
        etl_get_tweets(pipeline_conf, tweet_component_to_search)


# pylint: disable=unused-argument
def etl_replies_and_mentions(**kwargs):
    run_etl(
        EtlType.MentionsReplies,
        [
            TweetType.To, TweetType.Mention
        ]
    )


def etl_search_keywords(**kwargs):
    run_etl(
        EtlType.Keyword,
        [
            TweetType.To, TweetType.Mention, TweetType.From
        ]
    )


DAG_KEYWORD_REPLIES_KEYWORDS_DATA_PIPELINE_ID = (
    "Twitter_Keywords_Replies_And_Mentions_Data_Pipeline"
)
with DAG(
        dag_id=DAG_KEYWORD_REPLIES_KEYWORDS_DATA_PIPELINE_ID,
        default_args=get_default_args(),
        schedule_interval=None,
        dagrun_timeout=None,
) as tweet_mention_reply:
    # pylint: disable=expression-not-assigned
    [
        PythonOperator(
            task_id='twitter_data_etl_tweets_mention_replies',
            python_callable=etl_replies_and_mentions,
            execution_timeout=None
        ),
        PythonOperator(
            task_id='twitter_keyword_search_etl',
            python_callable=etl_search_keywords,
            execution_timeout=None
        )
    ]
