from typing import List

from GetOldTweets3.models import Tweet

from data_pipeline.twitter.twitter_config import (
    TwitterPipelineModuleConstants, ETL_TIMESTAMP_FORMAT, TweetType
)
from data_pipeline.utils.record_processing import standardize_record_keys


def extract_tweet_from_twitter_response(record):
    extracted_tweet = {
        TwitterPipelineModuleConstants.TWEET_AUTHOR_ID:
            record.user.id,
        TwitterPipelineModuleConstants.TWEET_AUTHOR_USERNAME:
            record.user.screen_name,
        TwitterPipelineModuleConstants.TWEETS_AUTHOR_FOLLOWERS_COUNT:
            record.user.followers_count,
        TwitterPipelineModuleConstants.TWEET_ID:
            record.id,
        TwitterPipelineModuleConstants.FORMATTED_DATE:
            record.created_at.strftime(ETL_TIMESTAMP_FORMAT),
        TwitterPipelineModuleConstants.TWEET_TEXT:
            record.text
            if hasattr(record, 'text')
            else record.full_text,
        TwitterPipelineModuleConstants.REPLIED_TO_TWEET_ID:
            record.in_reply_to_status_id,
        TwitterPipelineModuleConstants.REPLIED_TO_USER_ID:
            record.in_reply_to_user_id,
        TwitterPipelineModuleConstants.TWEET_URLS:
            (
                [
                    url_in_tweet['expanded_url'] for url_in_tweet in (
                        record.entities.get('urls', [])
                    )
                ]
            ),
        TwitterPipelineModuleConstants.USER_MENTIONS:
            [
                {
                    TwitterPipelineModuleConstants.USER_ID:
                        mention.get('id'),
                    TwitterPipelineModuleConstants.USER_SCREEN_NAME:
                        mention.get('screen_name')
                } for mention in (
                    record.user_mentions
                    if hasattr(record, 'user_mentions')
                    else record.entities.get('user_mentions', [])
                )
            ],
        TwitterPipelineModuleConstants.RETWEETS_COUNT:
            record.retweet_count,
        TwitterPipelineModuleConstants.FAVORITES_COUNT:
            record.favorite_count,
        TwitterPipelineModuleConstants.RETWEETED_TWEET_ID:
            record.retweeted_status.id
            if hasattr(record, 'retweeted_status')
            else None,
    }
    return standardize_record_keys(extracted_tweet)


def extract_tweet_properties_to_dict(tweet: Tweet):
    extracted_tweet = {
        TwitterPipelineModuleConstants.TWEET_ID: tweet.id,
        TwitterPipelineModuleConstants.FORMATTED_DATE:
            tweet.date.strftime(ETL_TIMESTAMP_FORMAT),
        TwitterPipelineModuleConstants.TWEET_TO: tweet.to,
        TwitterPipelineModuleConstants.TWEET_LINK:
            tweet.permalink,
        TwitterPipelineModuleConstants.REPLIES_COUNT:
            tweet.replies,
        TwitterPipelineModuleConstants.RETWEETS_COUNT:
            tweet.retweets,
        TwitterPipelineModuleConstants.FAVORITES_COUNT:
            tweet.favorites,
        TwitterPipelineModuleConstants.USER_MENTIONS:
            [
                {
                    TwitterPipelineModuleConstants.USER_SCREEN_NAME:
                        mention
                } for mention in tweet.mentions.split('@')
            ] if tweet.mentions else [],
        TwitterPipelineModuleConstants.TWEET_TEXT: tweet.text,
        TwitterPipelineModuleConstants.TWEET_AUTHOR_USERNAME:
            tweet.username,
        TwitterPipelineModuleConstants.TWEET_AUTHOR_ID: tweet.author_id,
        TwitterPipelineModuleConstants.TWEET_URLS:
            tweet.urls,
        TwitterPipelineModuleConstants.USER_LOCATION: tweet.geo,
    }
    return standardize_record_keys(extracted_tweet)


def modify_by_search_term_occurrence_location_in_response(
        jsonl: dict, search_term: str,
        tweet_type_enum_list: List[TweetType]
):
    search_term_with_at = (
        search_term.lower()
        if search_term.startswith('@')
        else '@' + search_term.lower()
    )

    tweet_author = jsonl.get(
        TwitterPipelineModuleConstants.TWEET_AUTHOR_USERNAME
    )
    tweet_author = (
        tweet_author.lower()
        if tweet_author.startswith('@')
        else '@' + tweet_author.lower()
    )
    reply_to = jsonl.get(TwitterPipelineModuleConstants.TWEET_TO)
    reply_to = (
        '@' + reply_to
        if reply_to and not reply_to.startswith('@')
        else reply_to
    )
    reply_to = reply_to.lower() if reply_to else reply_to
    tweet_text = jsonl.get(TwitterPipelineModuleConstants.TWEET_TEXT)
    tweet_type_key = TwitterPipelineModuleConstants.TWEET_TYPE
    tweet_type = []

    if (
            TweetType.From in tweet_type_enum_list
            and tweet_author == search_term_with_at
    ):
        tweet_type.append(TweetType.From.value)
    if (
            TweetType.To in tweet_type_enum_list
            and reply_to == search_term_with_at
    ):
        tweet_type.append(TweetType.To.value)
    if (
            TweetType.Retweet in tweet_type_enum_list
            and tweet_text.lower().startswith(
                'rt ' + search_term_with_at.lower() + ':'
            )
    ):
        tweet_type.append(TweetType.Retweet.value)
    elif (
            TweetType.Mention in tweet_type_enum_list
            and search_term.lower() in tweet_text.lower()
    ):
        tweet_type.append(TweetType.Mention.value)
    if TweetType.Other in tweet_type_enum_list and len(tweet_type) == 0:
        tweet_type.append(TweetType.Other.value)
    n_jsonl = {
        **jsonl,
        tweet_type_key: tweet_type,
    } if len(tweet_type) > 0 else {}

    return n_jsonl
