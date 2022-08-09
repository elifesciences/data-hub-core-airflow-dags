from datetime import date, datetime, timedelta
import logging
from typing import Any, Mapping, Optional
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
from data_pipeline.utils.pipeline_utils import (
    fetch_single_column_value_list_for_bigquery_source_config
)

LOGGER = logging.getLogger(__name__)


def get_client_from_twitter_ads_api(
    source_config: TwitterAdsApiSourceConfig
) -> Client:
    return Client(
        consumer_key=source_config.secrets.mapping['api_key'],
        consumer_secret=source_config.secrets.mapping['api_secret'],
        access_token=source_config.secrets.mapping['access_token'],
        access_token_secret=source_config.secrets.mapping['access_token_secret']
    )


def get_bq_compatible_json_response_from_resource_with_provenance(
    source_config: TwitterAdsApiSourceConfig,
    params_dict: Optional[Mapping[str, str]] = None
) -> Any:
    LOGGER.info("Getting data for resource: %s", source_config.resource)
    req = Request(
        client=get_client_from_twitter_ads_api(source_config=source_config),
        method="GET",
        resource=source_config.resource,
        params=params_dict
    )
    provenance = {
        'imported_timestamp': datetime.utcnow().isoformat(),
        'request_resource': source_config.resource
    }
    if params_dict:
        provenance['request_params'] = [
            {'name': key, 'value': value} for key, value in params_dict.items()
        ]
    response = req.perform()
    return {
        **remove_key_with_null_value(response.body),
        'provenance': provenance
    }


def iter_bq_compatible_json_response_from_resource_with_provenance(
    source_config: TwitterAdsApiSourceConfig
) -> Any:
    if source_config.param_from_bigquery and source_config.param_names:
        value_list_from_bq = fetch_single_column_value_list_for_bigquery_source_config(
            source_config.param_from_bigquery
        )
        LOGGER.debug('value_list_from_bq: %r', value_list_from_bq)
        today = date.today()
        yesterday = today - timedelta(days=1)

        for value_from_bq in value_list_from_bq:
            param_name_list = source_config.param_names
            LOGGER.info('param_name_list: %r', param_name_list)
            if param_name_list:
                params_dict = {
                    param_name_list[0]: value_from_bq,
                    # earliest campaign creation date for initial load:
                    param_name_list[1]: '2018-05-01',
                    param_name_list[2]: yesterday
                }

                yield get_bq_compatible_json_response_from_resource_with_provenance(
                    source_config=source_config,
                    params_dict=params_dict
                )

    yield get_bq_compatible_json_response_from_resource_with_provenance(
        source_config=source_config,
        params_dict=None
    )


def fetch_twitter_ads_api_data_and_load_into_bq(
    config: TwitterAdsApiConfig
):
    iterable_data_from_twitter_ads_api = (
        iter_bq_compatible_json_response_from_resource_with_provenance(
            source_config=config.source
        )
    )
    load_given_json_list_data_from_tempdir_to_bq(
        project_name=config.target.project_name,
        dataset_name=config.target.dataset_name,
        table_name=config.target.table_name,
        json_list=iterable_data_from_twitter_ads_api
    )
