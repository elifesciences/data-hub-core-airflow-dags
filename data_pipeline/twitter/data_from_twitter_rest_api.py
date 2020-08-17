import time
import logging
from typing import Iterable

import tweepy
from google.cloud.bigquery.client import Client
from tweepy import API, User
from tweepy.cursor import BaseIterator

from data_pipeline.twitter.process_tweet_response import (
    extract_tweet_from_twitter_response,
    modify_by_search_term_occurrence_location_in_response
)
from data_pipeline.twitter.twitter_bq_util import (
    batch_load_iter_record_to_bq, batch_load_multi_iter_record_to_bq
)
from data_pipeline.twitter.twitter_config import (
    TwitterDataPipelineConfig, TweetType,
    TwitterPipelineModuleConstants, ETL_TIMESTAMP_FORMAT
)
from data_pipeline.utils.data_store.bq_data_service import get_bq_table
from data_pipeline.utils.pipeline_file_io import WriteIterRecordsInBQConfig

from data_pipeline.utils.record_processing import (
    add_provenance_to_record, standardize_record_keys
)

LOGGER = logging.getLogger(__name__)


def _limit_handled(cursor: BaseIterator):
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            time.sleep(15 * 60)
        except StopIteration:
            break


class TwitterRestApi:
    def __init__(
            self, auth_oauth
    ):
        self.tweepy_api = API(auth_oauth)

    def get_api_restricted_followers_of_user(self, twitter_username: str):
        twitter_username = twitter_username.rstrip('@')
        for followers in _limit_handled(
                tweepy.Cursor(
                    self.tweepy_api.followers, id=twitter_username, count=1000
                ).pages()
        ):
            for follower in followers:
                yield extract_user_properties_to_dict(follower)

    def get_latest_api_restricted_tweets_from_user(self, twitter_username):
        twitter_username = twitter_username.rstrip('@')
        for tweets in _limit_handled(
                tweepy.Cursor(
                    self.tweepy_api.user_timeline, id=twitter_username
                ).pages()
        ):
            for tweet in tweets:
                yield tweet

    def get_all_retweets_of_a_tweet(self, tweet_id: int):
        try:
            retweets_list = self.tweepy_api.retweets(
                tweet_id, tweet_mode='extended', count=10000
            )
        except tweepy.RateLimitError:
            time.sleep(15 * 60)
            retweets_list = self.tweepy_api.retweets(
                tweet_id, tweet_mode='extended', count=10000
            )

        for retweet in retweets_list:
            yield retweet

    def get_all_retweets_of_all_api_restricted_tweets_of_user(
            self, twitter_username: str
    ):
        twitter_username = twitter_username.rstrip('@')
        for tweets in _limit_handled(
                tweepy.Cursor(
                    self.tweepy_api.user_timeline, id=twitter_username
                ).pages()
        ):
            for tweet in tweets:

                tweet_id = tweet.id
                for retweet in self.get_all_retweets_of_a_tweet(tweet_id):
                    yield retweet


# pylint:  disable=too-many-arguments
def iter_process_twitter_user(
        twitter_config: TwitterDataPipelineConfig,
        users, referenced_twitter_user: str = None,
        relationship_to_ref_user: str = None,
        latest_data_pipeline_timestamp: str = None
):
    u_ind = 0
    for user in users:
        if (u_ind % 50) == 0:
            LOGGER.info(
                "processed %d followers of %s",
                u_ind, referenced_twitter_user
            )
        u_ind += 1
        processed_data = add_provenance_to_record(
            user, latest_data_pipeline_timestamp,
            twitter_config.timestamp_field_name,
            twitter_config.provenance_field_name,
        )
        if referenced_twitter_user:
            ref_user = {
                'username': referenced_twitter_user,
                'relationship': relationship_to_ref_user
            }
            processed_data['referenced_twitter_user'] = ref_user
            processed_data = standardize_record_keys(processed_data)

        yield processed_data


def iter_process_retweets(
        twitter_config: TwitterDataPipelineConfig,
        retweets, referenced_twitter_user: str = None,
        latest_data_pipeline_timestamp: str = None
):
    r_ind = 0
    for retweet in retweets:
        if (r_ind % 200) == 0:
            LOGGER.info(
                "processed %d retweets  of tweets of user %s",
                r_ind, referenced_twitter_user
            )
        r_ind += 1
        retweet, original_tweet, retweeter = (
            extract_retweeter_and_tweets(retweet)
        )
        # TweetType.Mention
        original_tweet = modify_by_search_term_occurrence_location_in_response(
            original_tweet, referenced_twitter_user,
            [TweetType.To, TweetType.From, TweetType.Retweet, ]
        )
        retweet = modify_by_search_term_occurrence_location_in_response(
            retweet, referenced_twitter_user,
            [TweetType.To, TweetType.From, TweetType.Retweet]
        )
        retweet, original_tweet, retweeter = tuple(
            [
                standardize_record_keys(add_provenance_to_record(
                    entity, latest_data_pipeline_timestamp,
                    twitter_config.timestamp_field_name,
                    twitter_config.provenance_field_name,
                )) for entity in [retweet, original_tweet, retweeter]
            ]
        )
        if referenced_twitter_user:
            ref_user = {
                'username': referenced_twitter_user,
                'relationship': 'retweets'
            }
            retweeter['referenced_twitter_user'] = ref_user

        yield retweet, original_tweet, retweeter


def etl_user_followers(
        user_name: str,
        tweepy_api: TwitterRestApi,
        twitter_config: TwitterDataPipelineConfig,
        latest_data_pipeline_timestamp: str
):
    iter_user_followers = (
        tweepy_api.get_api_restricted_followers_of_user(
            user_name
        )
    )
    iter_processed_user_followers = iter_process_twitter_user(
        twitter_config, iter_user_followers, user_name,
        'following', latest_data_pipeline_timestamp
    )
    iter_written_records = batch_load_iter_record_to_bq(
        iter_processed_user_followers,
        twitter_config.gcp_project,
        twitter_config.dataset,
        twitter_config.user_table,
        batch_size=200
    )
    for _ in iter_written_records:
        continue


def extract_user_properties_to_dict(user: User):
    return {
        TwitterPipelineModuleConstants.USER_ID: user.id,
        TwitterPipelineModuleConstants.USER_NAME: user.name,
        TwitterPipelineModuleConstants.USER_SCREEN_NAME:
            user.screen_name,
        TwitterPipelineModuleConstants.USER_LOCATION: user.location,
        TwitterPipelineModuleConstants.DESCRIPTION: user.description,
        TwitterPipelineModuleConstants.FOLLOWERS_COUNT:
            user.followers_count,
        TwitterPipelineModuleConstants.FRIENDS_COUNT: user.friends_count,
        TwitterPipelineModuleConstants.LISTED_COUNT: user.listed_count,
        TwitterPipelineModuleConstants.FAVORITES_COUNT: user.favourites_count,
        TwitterPipelineModuleConstants.STATUSES_COUNT: user.statuses_count,
        TwitterPipelineModuleConstants.FORMATTED_DATE:
            user.created_at.strftime(ETL_TIMESTAMP_FORMAT)
    }


# pylint:  disable=too-many-arguments
def extract_retweeter_and_tweets(full_retweet):
    retweet = extract_tweet_from_twitter_response(full_retweet)
    original_tweet = extract_tweet_from_twitter_response(
        full_retweet.retweeted_status
    )
    retweeter = extract_user_properties_to_dict(full_retweet.user)

    return retweet, original_tweet, retweeter


def etl_user_retweets(
        user_name: str,
        tweepy_api: TwitterRestApi,
        twitter_config: TwitterDataPipelineConfig,
        latest_data_pipeline_timestamp: str
):
    iter_user_followers = (
        tweepy_api.get_all_retweets_of_all_api_restricted_tweets_of_user(
            user_name
        )
    )
    iter_processed_records = iter_process_retweets(
        twitter_config,
        iter_user_followers, user_name,
        latest_data_pipeline_timestamp
    )
    iter_written_records = iter_stream_write_entities_from_retweets(
        iter_processed_records,
        twitter_config
    )
    # pylint: disable=pointless-string-statement
    '''
    iter_written_records = iter_batch_load_entities_from_retweets(
        iter_processed_records,
        twitter_config
    )
    '''
    for _ in iter_written_records:
        continue


def iter_stream_write_entities_from_retweets(
        iter_processed_records,
        twitter_config: TwitterDataPipelineConfig,

):
    bq_client = Client(project=twitter_config.gcp_project)
    tweet_table = get_bq_table(
        twitter_config.gcp_project,
        twitter_config.dataset,
        twitter_config.tweet_table
    )
    user_table = get_bq_table(
        twitter_config.gcp_project,
        twitter_config.dataset,
        twitter_config.user_table
    )
    for record in iter_processed_records:
        bq_client.insert_rows_json(
            table=tweet_table, json_rows=[record[0], record[1]]
        )
        bq_client.insert_rows_json(
            table=user_table, json_rows=[record[2]]
        )
        yield record


def iter_batch_load_entities_from_retweets(
        iter_processed_records: Iterable,
        twitter_config: TwitterDataPipelineConfig,
):
    retweet_write_config = [
        WriteIterRecordsInBQConfig(twitter_config.tweet_table),
        WriteIterRecordsInBQConfig(twitter_config.tweet_table),
        WriteIterRecordsInBQConfig(twitter_config.user_table)
    ]

    iter_written_records = batch_load_multi_iter_record_to_bq(
        iter_processed_records,
        retweet_write_config,
        twitter_config.gcp_project,
        twitter_config.dataset,
        batch_size=5000
    )
    yield from iter_written_records
