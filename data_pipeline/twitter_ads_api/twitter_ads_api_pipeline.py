from typing import Any
from twitter_ads.http import Request
from twitter_ads.client import Client

from data_pipeline.twitter_ads_api.twitter_ads_api_config import (
    TwitterAdsApiConfig,
    TwitterAdsApiSourceConfig
)
from data_pipeline.utils.data_store.bq_data_service import (
    load_given_json_list_data_from_tempdir_to_bq
)
from data_pipeline.utils.json import remove_key_with_null_value


def get_client_from_twitter_ads_api(
    source_config: TwitterAdsApiSourceConfig
) -> Client:
    return Client(
        consumer_key=source_config.secrets.mapping['api_key'],
        consumer_secret=source_config.secrets.mapping['api_secret'],
        access_token=source_config.secrets.mapping['access_token'],
        access_token_secret=source_config.secrets.mapping['access_token_secret']
    )


def get_bq_compatible_json_response_from_resource(
    source_config: TwitterAdsApiSourceConfig
) -> Any:
    req = Request(
        client=get_client_from_twitter_ads_api(source_config=source_config),
        method="GET",
        resource=source_config.resource
    )
    response = req.perform()
    return remove_key_with_null_value(response.body)


def fetch_twitter_ads_api_data_and_load_into_bq(
    config: TwitterAdsApiConfig
):
    data_from_twitter_ads_api = get_bq_compatible_json_response_from_resource(
        source_config=config.source
    )
    load_given_json_list_data_from_tempdir_to_bq(
        project_name=config.target.project_name,
        dataset_name=config.target.dataset_name,
        table_name=config.target.table_name,
        json_list=[data_from_twitter_ads_api]
    )
