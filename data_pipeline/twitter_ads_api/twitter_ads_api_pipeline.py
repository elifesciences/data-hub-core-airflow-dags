from datetime import datetime, timedelta
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
    fetch_single_column_value_list_for_bigquery_source_config,
    iter_dict_from_bq_query_for_bigquery_source_config
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
    api_query_parameters_config: TwitterAdsApiApiQueryParametersConfig,
    entity_id: str,
    start_date: str,
    end_date: str,
    placement: Optional[str] = None
) -> dict:
    result = {
        api_query_parameters_config.parameter_names_for.entity_id: entity_id,
        api_query_parameters_config.parameter_names_for.start_date: start_date,
        api_query_parameters_config.parameter_names_for.end_date: end_date
    }
    if placement:
        result[api_query_parameters_config.parameter_names_for.placement] = placement
    return result


def get_start_date_and_period_end_date_dict(
    api_query_parameters_config: TwitterAdsApiApiQueryParametersConfig, 
    start_date_value_from_bq: str
) -> dict:
    start_date_value = (
        datetime.strptime(start_date_value_from_bq, '%Y-%m-%d').date()
    )
    LOGGER.debug("start_date_value: %r", )
    yesterday_date = get_yesterdays_date()
    LOGGER.debug("yesterday: %r", yesterday_date)
    end_date_by_period = start_date_value + timedelta(
        days=api_query_parameters_config.parameter_values.ending_period_per_day
    )
    LOGGER.debug("end_date_by_period: %r", end_date_by_period)
    if end_date_by_period > yesterday_date:
        end_date_value = yesterday_date
    else:
        end_date_value = end_date_by_period
    LOGGER.debug("end_date_value: %r", end_date_value)
    return {'start_date': start_date_value, 'period_end_date': end_date_value}


def iter_bq_compatible_json_response_from_resource_with_provenance(
    source_config: TwitterAdsApiSourceConfig
) -> Any:
    if source_config.api_query_parameters:
        api_query_parameters_config = source_config.api_query_parameters
        placement_value_list = api_query_parameters_config.parameter_values.placement_value
        if api_query_parameters_config.parameter_names_for.placement:
            assert placement_value_list
            dict_value_list_from_bq = list(iter_dict_from_bq_query_for_bigquery_source_config(
                api_query_parameters_config.parameter_values.from_bigquery
            ))
            LOGGER.debug("dict_value_list_from_bq: %r", dict_value_list_from_bq)
            for dict_value_from_bq in dict_value_list_from_bq:
                for placement_value in placement_value_list:
                    start_and_end_date_dict = get_start_date_and_period_end_date_dict(
                        api_query_parameters_config=api_query_parameters_config,
                        start_date_value_from_bq=(
                            datetime.strptime(dict_value_from_bq['start_date'], '%Y-%m-%d').date()
                        )
                    )
                    start_date_value = start_and_end_date_dict['start_date']
                    period_end_date_value = start_and_end_date_dict['period_end_date']
                    while start_date_value < period_end_date_value:
                        seven_days_after_start_date_value = start_date_value + timedelta(days=7)
                        if seven_days_after_start_date_value > period_end_date_value:
                            end_date_value = period_end_date_value
                        else:
                            end_date_value = seven_days_after_start_date_value
                        params_dict = get_param_dict_from_api_query_parameters(
                            api_query_parameters_config=api_query_parameters_config,
                            entity_id=dict_value_from_bq['entity_id'],
                            start_date=start_date_value.isoformat(),
                            end_date=end_date_value.isoformat(),
                            placement=placement_value
                        )
                        start_date_value = end_date_value

                        yield get_bq_compatible_json_response_from_resource_with_provenance(
                                source_config=source_config,
                                params_dict=params_dict
                            )
        else:
            value_list_from_bq = fetch_single_column_value_list_for_bigquery_source_config(
                api_query_parameters_config.parameter_values.from_bigquery
            )
            for value_from_bq in value_list_from_bq:
                params_dict = get_param_dict_from_api_query_parameters(
                    api_query_parameters_config=api_query_parameters_config,
                    entity_id=value_from_bq,
                    start_date=api_query_parameters_config.parameter_values.start_date_value,
                    end_date=get_yesterdays_date().isoformat()
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
