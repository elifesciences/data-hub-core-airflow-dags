import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from data_pipeline.twitter.data_from_twitter_rest_api import TwitterRestApi
from data_pipeline.twitter.etl_twitter import etl_get_tweets, etl_get_users_followers
from data_pipeline.twitter.twiter_auth import get_authorized_oauth
from data_pipeline.twitter.twitter_config import TweetType
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    get_default_args,
)
from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict


TWITTER_USER_FOLLOWER_CONFIG_FILE_PATH_ENV_NAME = (
    "TWITTER_USER_FOLLOWER_CONFIG_FILE_PATH"
)
TWITTER_USER_FOLLOWER_PIPELINE_SCHEDULE_INTERVAL_ENV_NAME = (
    "TWITTER_USER_FOLLOWER_PIPELINE_SCHEDULE_INTERVAL"
)

DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"


def get_data_config(**kwargs):
    conf_file_path = os.getenv(
        TWITTER_USER_FOLLOWER_CONFIG_FILE_PATH_ENV_NAME
    )
    data_config_dict = get_yaml_file_as_dict(
        conf_file_path
    )
    kwargs["ti"].xcom_push(
        key="multi_twitter_user_follower_config_dict",
        value=data_config_dict
    )


def get_users_tweets(**kwargs):
    bb = {
        'gcpProjectName': 'elife-data-pipeline',
        'dataset': 'me_dev',
        'table': 'tweetss',
        'stateFile': {
            'bucketName': 'ci-elife-data-pipeline',
            'objectName': 'airflow-config/twitter/state/ci-twitter-user-names-profile-statesds'
        },
        'defaultStartTime': '2018-01-01'
    }
    user_names = ['@eLife', '@eLifecommunity', '@eLifeinnovation',  '@PreprintReview']
    tweet_component_to_search = [TweetType.From, TweetType.To, TweetType.Mention]
    etl_get_tweets(bb, user_names, tweet_component_to_search)


def etl_search_keywords(**kwargs):
    bb = {
        'gcpProjectName': 'elife-data-pipeline',
        'dataset': 'me_dev',
        'table': 'tweetss',
        'stateFile': {
            'bucketName': 'ci-elife-data-pipeline',
            'objectName': 'airflow-config/twitter/state/ci-twitter-user-names-profile-statesds'
        },
        'defaultStartTime': '2018-01-01'
    }
    search_terms = [
        '#virology', '#bacteriology', '#pathology', '#parasitology',
        '#mycology', '#microbes', '#microbiome', '#Phytopathology'
    ]
    etl_get_tweets(bb, search_terms)


def get_users_followers(**kwargs):
    # Go to http://apps.twitter.com and create an app.
    # The consumer key and secret will be generated for you after
    consumer_key = "jF7JrEXzL7VdhlxnSbLkCQgon"
    consumer_secret = "IhJRbuLSRbz25j5Lii5UfA6iMaLXT8w5U4zSzKUCDFpbqNcYVa"

    # After the step above, you will be redirected to your app's page.
    # Create an access token under the the "Your access token" section
    access_token = "1251350288-liFvPWSIs6e8ywEsJAaBVOmK2yvVlLIMbODY138"
    access_token_secret = 'kYzE2f48QLyqJZfhqxcuNKEn6zEKmLzU0M8ZoryKNGtsJ'
    auth = get_authorized_oauth(
        consumer_key, consumer_secret,
        access_token, access_token_secret
    )
    t =TwitterRestApi(auth)
    bb = {
        'gcpProjectName': 'elife-data-pipeline',
        'dataset': 'me_dev',
        'table': 'tst_twitter_ufollwe1',
        'stateFile': {
            'bucketName': 'ci-elife-data-pipeline',
            'objectName': 'airflow-config/twitter/state/ci-twitter-user-followerssd'
        },
        'defaultStartTime': '2018-01-01 10:00:00'
    }
    user_names = ['@eLife', '@eLifecommunity', '@eLifeinnovation',  '@PreprintReview']
    etl_get_users_followers(bb, user_names, t)


DAG_USER_FOLLOWERS_PIPELINE_ID = "Twitter_User_Followers_Data_Pipeline"
with DAG(
    dag_id=DAG_USER_FOLLOWERS_PIPELINE_ID,
    default_args=get_default_args(),
    schedule_interval=None,
    dagrun_timeout=None,
    concurrency=1,
) as twitter_followers_dag:
    PythonOperator(
        task_id='twitter_data_etl_followers',
        python_callable=get_users_followers,
        execution_timeout=None
    )


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
