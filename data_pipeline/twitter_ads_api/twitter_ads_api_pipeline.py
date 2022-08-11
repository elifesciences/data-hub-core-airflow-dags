from datetime import date, datetime, timedelta
import logging
from typing import Any, Mapping, Optional, Sequence
from twitter_ads.http import Request
from twitter_ads.client import Client

from data_pipeline.twitter_ads_api.twitter_ads_api_config import (
    TwitterAdsApiApiQueryParametersConfig,
    TwitterAdsApiConfig,
    TwitterAdsApiSourceConfig
)
from data_pipeline.utils.data_pipeline_timestamp import get_yesterdays_date
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


def get_provenance(
    source_config: TwitterAdsApiSourceConfig,
    params_dict: Optional[Mapping[str, str]] = None
) -> dict:
    provenance = {
        'imported_timestamp': datetime.utcnow().isoformat(),
        'request_resource': source_config.resource
    }
    if params_dict:
        provenance['request_params'] = [
            {'name': key, 'value': value} for key, value in params_dict.items()
        ]
    LOGGER.debug('provenance: %r', provenance)
    return provenance


def get_bq_compatible_json_response_from_resource_with_provenance(
    source_config: TwitterAdsApiSourceConfig,
    params_dict: Optional[Mapping[str, str]] = None
) -> Any:
    LOGGER.info("Getting data for resource: %s", source_config.resource)
    LOGGER.info('params_dict: %r', params_dict)
    req = Request(
        client=get_client_from_twitter_ads_api(source_config=source_config),
        method="GET",
        resource=source_config.resource,
        params=params_dict
    )
    response = req.perform()
    return remove_key_with_null_value({
        **response.body,
        'provenance': get_provenance(source_config, params_dict)
    })


def get_param_dict_from_api_query_parameters(
    api_query_parameters: TwitterAdsApiApiQueryParametersConfig,
    value_from_bq: str,
    start_time: str,
    end_time: str
) -> dict:
    return {
        api_query_parameters.parameter_names_for.bigquery_value: value_from_bq,
        api_query_parameters.parameter_names_for.start_time: start_time,
        api_query_parameters.parameter_names_for.end_time: end_time
    }


def get_param_dict_from_api_query_parameters_v2(
    api_query_parameters_config: TwitterAdsApiApiQueryParametersConfig,
) -> dict:
    value_list_from_bq = fetch_single_column_value_list_for_bigquery_source_config(
        api_query_parameters_config.parameter_values.from_bigquery
    )
    today = date.today()
    yesterday = today - timedelta(days=1)
    for value_from_bq in value_list_from_bq:
        return {
            api_query_parameters_config.parameter_names_for.bigquery_value: value_from_bq,
            api_query_parameters_config.parameter_names_for.start_time: (
                api_query_parameters_config.parameter_values.start_time_value
            ),
            api_query_parameters_config.parameter_names_for.end_time: yesterday.isoformat()
        }


def iter_bq_compatible_json_response_from_resource_with_provenance(
    source_config: TwitterAdsApiSourceConfig
) -> Any:
    if source_config.api_query_parameters:
        value_list_from_bq = fetch_single_column_value_list_for_bigquery_source_config(
            source_config.api_query_parameters.parameter_values.from_bigquery
        )
        for value_from_bq in value_list_from_bq:
            params_dict = get_param_dict_from_api_query_parameters(
                api_query_parameters=source_config.api_query_parameters,
                value_from_bq=value_from_bq,
                start_time=source_config.api_query_parameters.parameter_values.start_time_value,
                end_time=get_yesterdays_date().isoformat()
            )

            yield get_bq_compatible_json_response_from_resource_with_provenance(
                    source_config=source_config,
                    params_dict=params_dict
                )
    else:
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


def fetch_twitter_ads_api_data_and_load_into_bq_from_config_list(
    config_list: Sequence[TwitterAdsApiConfig]
):
    for config in config_list:
        fetch_twitter_ads_api_data_and_load_into_bq(config)
