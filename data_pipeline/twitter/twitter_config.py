import enum
import os

from data_pipeline.utils.pipeline_config import (
    update_deployment_env_placeholder
)
from data_pipeline.utils.pipeline_file_io import get_data_config_from_file_path_in_env_var

ETL_TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'
DEFAULT_TIMESTAMP_FIELD_NAME = 'data_hub_timestamp'
DEFAULT_PROVENANCE_FIELD_NAME = 'provenance'


class TweetType(enum.Enum):
    Mention = 'Tweet Mention'
    From = 'Tweeted From'
    To = 'Tweet To'
    Other = 'Other'
    Retweet = 'Retweet'


class EtlType(enum.Enum):
    Keyword = 'keyword'
    Username = 'username'
    Followers = 'followers'


class MultiTwitterPipelineConfig:
    def __init__(self,
                 multi_twitter_pipeline_config: dict,
                 deployment_env: str
                 ):
        gcp_project = multi_twitter_pipeline_config.get("gcpProjectName")
        import_timestamp_field_name = multi_twitter_pipeline_config.get(
            "importedTimestampFieldName", 'data_hub_import_timestamp'
        )
        provenance_field_name = multi_twitter_pipeline_config.get(
            "provenanceFieldName", 'data_hub_provenance'
        )
        self.twitter_pipeline_config = [
            TwitterDataPipelineConfig(
                t_pipeline, gcp_project, deployment_env,
                provenance_field_name=provenance_field_name,
                timestamp_field_name= import_timestamp_field_name
            )
            for t_pipeline in multi_twitter_pipeline_config.get("twitterPipeline")
        ]


class TwitterDataPipelineConfig:
    def __init__(
            self,
            twitter_config: dict,
            gcp_project: str = None,
            deployment_env: str = None,
            deployment_env_placeholder: str = "{ENV}",
            provenance_field_name: str = None,
            timestamp_field_name: str = None,
    ):
        t_config = update_deployment_env_placeholder(
            twitter_config, deployment_env,
            deployment_env_placeholder
        ) if deployment_env else twitter_config
        self.gcp_project = t_config.get('gcpProjectName') or gcp_project
        self.dataset = t_config.get('dataset')
        self.tweet_table = t_config.get('tweet_table')
        self.user_table = t_config.get('user_table')
        self.etl_type = {
            'keyword': EtlType.Keyword,
            'username': EtlType.Username,
            'followers': EtlType.Followers
        }.get(t_config.get('etlType', 'keyword').lower())
        self.etl_terms = [
            (
                lambda x: '@' + x if self.etl_type == EtlType.Username and not x.startswith('@') else x
            )(etlTerms) for etlTerms in t_config.get('etlTerms', [])
        ]

        self.default_start_date = t_config.get('defaultStartTime')
        self.state_s3_bucket_name = t_config.get('stateFile', {}).get('bucketName')
        self.state_s3_object_name_prefix = t_config.get('stateFile', {}).get('objectNamePrefix')
        self.timestamp_field_name = timestamp_field_name if timestamp_field_name else DEFAULT_TIMESTAMP_FIELD_NAME
        self.provenance_field_name = provenance_field_name if provenance_field_name else DEFAULT_PROVENANCE_FIELD_NAME


class TwitterPipelineModuleConstants:
    TWEET_ID = 'tweet_id'
    TWEET_TEXT = 'tweet_text'
    FORMATTED_DATE = 'formatted_date'
    TWEET_TO = 'tweet_to_username'
    FRIENDS_COUNT = 'friends_count'
    FOLLOWERS_COUNT = 'followers_count'
    USER_LOCATION = 'user_location'
    USER_STATUS = 'user_status'
    RETWEETS_COUNT = 'retweets_count'
    REPLIES_COUNT = 'replies_count'
    TWEETS_AUTHOR_FOLLOWERS_COUNT = 'tweets_author_followers_count'
    FAVORITES_COUNT = 'favorites_count'
    TWEET_URLS = 'urls'
    TWEET_AUTHOR_USERNAME = 'author_username'
    TWEET_AUTHOR_ID = 'author_id'
    TWEET_MENTIONS = 'mentions'
    TWEET_LINK = 'permalink'
    TWEET_TYPE = 'tweet_type'
    RETWEETED_TWEET_ID = 'retweeted_tweet_id'
    QUOTED_STATUS_ID = 'quoted_status_id'
    USER_ID = 'user_id'
    USER_NAME = 'user_name'
    USER_MENTIONS = 'user_mentions'
    REPLIED_TO_TWEET_ID = 'replied_to_tweet_id'
    REPLIED_TO_USER_ID = 'replied_to_user_id'


def get_filtered_pipeline_conf(
        etl_type_filter: EtlType, deployment_env: str,
        env_var_with_file_path: str
):
    multi_etl_dict = get_data_config_from_file_path_in_env_var(
        env_var_with_file_path
    )
    deployment_env = os.getenv(deployment_env)

    multi_pipeline_conf = MultiTwitterPipelineConfig(multi_etl_dict, deployment_env)
    pipeline_conf_list = [
        u_name_conf for u_name_conf in multi_pipeline_conf.twitter_pipeline_config
        if u_name_conf.etl_type == etl_type_filter
    ]
    return pipeline_conf_list
