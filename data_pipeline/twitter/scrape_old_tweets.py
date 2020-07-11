import logging
from typing import List
from data_pipeline.twitter.get_old_tweet3_extended import TCriteria, TManager
from data_pipeline.twitter.process_tweet_response import extract_tweet_properties_to_dict, \
    modify_by_search_term_occurrence_location_in_response
from data_pipeline.twitter.twitter_bq_util import batch_load_iter_record_to_bq
from data_pipeline.twitter.etl_state import (
    update_state, get_s3_object_name_for_search_term,
    get_stored_state_for_object_id
)
from data_pipeline.twitter.twitter_config import (
    ETL_TIMESTAMP_FORMAT, TwitterPipelineModuleConstants,
    TweetType, TwitterDataPipelineConfig
)
from data_pipeline.utils.record_processing import (
    add_provenance_to_record, standardize_record_keys
)
from data_pipeline.utils.data_pipeline_timestamp import parse_timestamp_from_str

STATE_TIMESTAMP_FORMAT = '%Y-%m-%d'
DEFAULT_START_DATE = '2012-01-01'

LOGGER = logging.getLogger(__name__)


def iter_search_get_old_tweets(
        search_term: str = None, tweet_from: List = None,
        tweet_to: List = None, tweet_from_date: str = None,
):
    tweet_criteria = TCriteria()
    if search_term:
        tweet_criteria.setQuerySearch(search_term)
    if tweet_from:
        tweet_criteria = tweet_criteria.setUsername(tweet_from)
    if tweet_to:
        tweet_criteria = tweet_criteria.set_to_username(tweet_to)
    if tweet_from_date:
        tweet_criteria = tweet_criteria.setSince(tweet_from_date)

    tweets = TManager.get_iter_twitter_response(tweet_criteria)
    for tweet in tweets:
        yield tweet


# pylint:  disable=too-many-arguments
def iter_process_filter_tweet(
        twitter_config: TwitterDataPipelineConfig,
        tweets, search_term: str,
        tweet_type_filter_list: List[TweetType],
        latest_data_pipeline_timestamp: str = None
):
    t_count = 0
    for tweet in tweets:
        if t_count % 100:
            LOGGER.info('Tweets processed: %d ', t_count)
        tweet_as_dict = extract_tweet_properties_to_dict(tweet)
        processed_data = (
            modify_by_search_term_occurrence_location_in_response(
                tweet_as_dict, search_term,
                tweet_type_filter_list
            )
        )
        if not processed_data:
            continue
        processed_data['search_term'] = search_term
        processed_data = standardize_record_keys(add_provenance_to_record(
            processed_data, latest_data_pipeline_timestamp,
            twitter_config.timestamp_field_name,
            twitter_config.provenance_field_name,
        ))
        yield processed_data


def get_stored_state_for_term(search_term: str, twitter_config: TwitterDataPipelineConfig):
    s3_object = get_s3_object_name_for_search_term(
        twitter_config.state_s3_object_name_prefix,
        search_term
    )
    stored_state = get_stored_state_for_object_id(
        twitter_config.state_s3_bucket_name,
        s3_object,
        search_term,
        twitter_config.default_start_date
    ) or DEFAULT_START_DATE

    return stored_state


def etl_search_term_by_scraping(
        twitter_config: TwitterDataPipelineConfig,
        tweet_type_filter_list: List[TweetType],
        search_term: str = None,
        user_name: str = None,
        latest_data_pipeline_timestamp: str = None,
):
    stored_state = get_stored_state_for_term(search_term, twitter_config)
    if user_name and search_term is None:
        search_term = user_name.lstrip().lstrip('@')
    iter_tweets = iter_search_get_old_tweets(
        search_term=search_term,
        tweet_from=[user_name],
        tweet_from_date=stored_state
    )
    iter_processed_tweets = iter_process_filter_tweet(
        twitter_config=twitter_config,
        tweets=iter_tweets, search_term=search_term,
        latest_data_pipeline_timestamp=latest_data_pipeline_timestamp,
        tweet_type_filter_list=tweet_type_filter_list,
    )
    iter_written_records = batch_load_iter_record_to_bq(
        iter_processed_tweets,
        twitter_config.gcp_project,
        twitter_config.dataset,
        twitter_config.tweet_table
    )
    _, latest_tweet_date_str = get_latest_twitter_id_and_date(
        iter_written_records
    )
    upload_state(search_term, latest_tweet_date_str, twitter_config)


def upload_state(
        search_term: str,
        latest_tweet_date_str: str,
        twitter_config: TwitterDataPipelineConfig
):
    s3_object_name = get_s3_object_name_for_search_term(
        twitter_config.state_s3_object_name_prefix,
        search_term
    )

    update_state(
        search_term,
        latest_tweet_date_str,
        twitter_config.state_s3_bucket_name,
        s3_object_name
    )


def get_latest_twitter_id_and_date(iter_tweets):
    latest_tweet_id = 0
    latest_tweet_timestamp_as_str = None
    for tweet in iter_tweets:
        tweet_id = tweet.get(
            TwitterPipelineModuleConstants.TWEET_ID
        )
        if latest_tweet_id < int(tweet_id) and tweet.get(
                TwitterPipelineModuleConstants.FORMATTED_DATE
        ):
            latest_tweet_id = int(tweet_id)
            latest_tweet_timestamp_as_str = parse_timestamp_from_str(
                tweet.get(TwitterPipelineModuleConstants.FORMATTED_DATE),
                ETL_TIMESTAMP_FORMAT
            ).strftime(STATE_TIMESTAMP_FORMAT)
    return latest_tweet_id, latest_tweet_timestamp_as_str
