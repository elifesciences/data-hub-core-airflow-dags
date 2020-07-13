import os
from pathlib import Path
from typing import List
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from data_pipeline.twitter.data_from_twitter_rest_api import TwitterRestApi
from data_pipeline.twitter.etl_twitter import etl_get_users_followers, create_twitter_data_pipeline_tables
from data_pipeline.twitter.twiter_auth import get_authorized_from_file
from data_pipeline.twitter.twitter_config import (
    EtlType, get_filtered_pipeline_conf, TwitterDataPipelineConfig
)
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    get_default_args,
)

TWITTER_USER_FOLLOWERS_CONFIG_FILE_PATH_ENV_NAME = (
    "TWITTER_USER_FOLLOWERS_CONFIG_FILE_PATH"
)
TWITTER_USER_FOLLOWERS_PIPELINE_SCHEDULE_INTERVAL_ENV_NAME = (
    "TWITTER_USER_FOLLOWERS_PIPELINE_SCHEDULE_INTERVAL"
)
TWITTER_AUTH_YAML_FILE_ENV_NAME = (
    'TWITTER_AUTH_YAML_FILE'
)

AUX_FILES_DIRECTORY_ENV_NAME = (
    'AIRFLOW_APPLICATIONS_DIRECTORY_PATH'
)
DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"


def create_twitter_table_if_not_exist(**kwargs):
    pipeline_conf_list: List[TwitterDataPipelineConfig] = get_filtered_pipeline_conf(
        EtlType.Followers, DEPLOYMENT_ENV_ENV_NAME,
        TWITTER_USER_FOLLOWERS_CONFIG_FILE_PATH_ENV_NAME
    )

    user_table_schema_path = str(
        Path(
            os.getenv(AUX_FILES_DIRECTORY_ENV_NAME),
            'twitter', 'twitter_user.schema.json'
        )
    )

    create_twitter_data_pipeline_tables(
        pipeline_conf_list,
        user_table_schema_path=user_table_schema_path,
    )


# pylint: disable=unused-argument
def get_users_followers(**kwargs):
    auth = get_authorized_from_file(
        os.getenv(TWITTER_AUTH_YAML_FILE_ENV_NAME)
    )
    tweepy_auth = TwitterRestApi(auth)
    pipeline_conf_list = get_filtered_pipeline_conf(
        EtlType.Followers, DEPLOYMENT_ENV_ENV_NAME,
        TWITTER_USER_FOLLOWERS_CONFIG_FILE_PATH_ENV_NAME
    )
    for pipeline_conf in pipeline_conf_list:
        etl_get_users_followers(pipeline_conf, tweepy_auth)


DAG_USER_FOLLOWERS_PIPELINE_ID = "Twitter_User_Followers_Data_Pipeline"
with DAG(
        dag_id=DAG_USER_FOLLOWERS_PIPELINE_ID,
        default_args=get_default_args(),
        schedule_interval=None,
        dagrun_timeout=None,
) as twitter_followers_dag:
    PythonOperator(
        task_id='create_twitter_data_pipeline_table',
        python_callable=create_twitter_table_if_not_exist,
        execution_timeout=None
    ) >> PythonOperator(
        task_id='twitter_data_etl_followers',
        python_callable=get_users_followers,
        execution_timeout=None
    )
