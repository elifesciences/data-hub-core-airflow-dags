from typing import List
from data_pipeline.twitter.scrape_old_tweets import etl_search_term_by_scraping
from data_pipeline.twitter.twitter_config import (
    TwitterDataPipelineConfig, TweetType
)
from data_pipeline.utils.data_pipeline_timestamp import (
    get_current_timestamp_as_string
)
from data_pipeline.twitter.twitter_config import ETL_TIMESTAMP_FORMAT
from data_pipeline.twitter.data_from_twitter_rest_api import (
    etl_user_followers, TwitterRestApi, etl_user_retweets
)
from data_pipeline.utils.data_store.bq_data_service import (
    create_table_if_not_exist
)
from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict


def etl_get_tweets(
        pipeline_conf: TwitterDataPipelineConfig,
        tweet_component_to_search: List[TweetType] = None,
        etl_timestamp: str = None
):
    tweet_component_to_search = (
        tweet_component_to_search or [TweetType.Mention]
    )
    etl_timestamp = (
        etl_timestamp or get_current_timestamp_as_string(ETL_TIMESTAMP_FORMAT)
    )

    for search_term in pipeline_conf.etl_terms:
        etl_search_term_by_scraping(
            search_term=search_term, twitter_config=pipeline_conf,
            tweet_type_filter_list=tweet_component_to_search,
            latest_data_pipeline_timestamp=etl_timestamp
        )


def etl_get_user_tweets(
        pipeline_conf: TwitterDataPipelineConfig,
        etl_timestamp: str = None
):
    tweet_component_to_search = [TweetType.From]
    etl_timestamp = (
        etl_timestamp or get_current_timestamp_as_string(ETL_TIMESTAMP_FORMAT)
    )

    for user_name in pipeline_conf.etl_terms:
        etl_search_term_by_scraping(
            user_name=user_name, twitter_config=pipeline_conf,
            tweet_type_filter_list=tweet_component_to_search,
            latest_data_pipeline_timestamp=etl_timestamp
        )


def etl_get_users_followers(
        pipeline_config: TwitterDataPipelineConfig,
        tweepy_api: TwitterRestApi,
        from_date_as_str: str = None
):
    etl_timestamp = (
        from_date_as_str
        or get_current_timestamp_as_string(ETL_TIMESTAMP_FORMAT)
    )

    for twitter_user in pipeline_config.etl_terms:
        etl_user_followers(
            twitter_user, tweepy_api, pipeline_config, etl_timestamp
        )


def etl_get_users_retweets(
        pipeline_config: TwitterDataPipelineConfig,
        tweepy_api: TwitterRestApi,
        from_date_as_str: str = None
):
    etl_timestamp = (
        from_date_as_str
        or get_current_timestamp_as_string(ETL_TIMESTAMP_FORMAT)
    )

    for twitter_user in pipeline_config.etl_terms:
        etl_user_retweets(
            twitter_user, tweepy_api, pipeline_config, etl_timestamp
        )


def create_twitter_data_pipeline_tables(
        pipeline_conf_list: List[TwitterDataPipelineConfig],
        user_table_schema_path: str = None,
        tweet_table_schema_path: str = None
):
    user_table_schema = (
        get_yaml_file_as_dict(user_table_schema_path)
        if user_table_schema_path else None
    )
    tweet_table_schema = (
        get_yaml_file_as_dict(tweet_table_schema_path)
        if tweet_table_schema_path else None
    )

    for pipeline_conf in pipeline_conf_list:
        if user_table_schema_path:
            create_table_if_not_exist(
                project_name=pipeline_conf.gcp_project,
                dataset_name=pipeline_conf.dataset,
                table_name=pipeline_conf.user_table,
                json_schema=user_table_schema
            )
        if tweet_table_schema_path:
            create_table_if_not_exist(
                project_name=pipeline_conf.gcp_project,
                dataset_name=pipeline_conf.dataset,
                table_name=pipeline_conf.tweet_table,
                json_schema=tweet_table_schema
            )
