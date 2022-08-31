from datetime import date, datetime, timedelta
import logging
from typing import Any, Iterable, Mapping, Optional, Sequence
from twitter_ads.http import Request
from twitter_ads.client import Client

from data_pipeline.twitter_ads_api.twitter_ads_api_config import (
    TwitterAdsApiApiQueryParametersConfig,
    TwitterAdsApiConfig,
    TwitterAdsApiSourceConfig
)
from data_pipeline.utils.collections import iter_batches_iterable
from data_pipeline.utils.data_pipeline_timestamp import get_yesterdays_date
from data_pipeline.utils.data_store.bq_data_service import (
    load_given_json_list_data_from_tempdir_to_bq
)
from data_pipeline.utils.json import remove_key_with_null_value
from data_pipeline.utils.pipeline_utils import (
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
        access_token_secret=source_config.secrets.mapping['access_token_secret'],
        options={
            'handle_rate_limit': True,
            'retry_max': 10,
            'retry_delay': 5000,
            'retry_on_status': [500, 503, 504],
            'retry_on_timeouts': True,
            'timeout': (3.0, 5.0)
        }
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


def get_min_date(date1: date, date2: date) -> date:
    if date1 < date2:
        return date1
    return date2


def add_days_to_date(date_value: date, number_of_days_to_add: int) -> date:
    return date_value + timedelta(days=number_of_days_to_add)


def get_current_final_end_date(
    api_query_parameters_config: TwitterAdsApiApiQueryParametersConfig,
    initial_start_date: date
) -> date:
    yesterday_date = get_yesterdays_date()
    period_max_end_date = add_days_to_date(
        initial_start_date,
        api_query_parameters_config.parameter_values.max_period_in_days
    )
    return get_min_date(yesterday_date, period_max_end_date)


def get_end_date_value_of_batch_period(
    start_date: date,
    final_end_date: date,
    batch_size_in_days: int
) -> date:
    period_end_date = add_days_to_date(start_date, batch_size_in_days)
    return get_min_date(period_end_date, final_end_date)


def iter_bq_compatible_json_response_from_resource_with_provenance(
    source_config: TwitterAdsApiSourceConfig
) -> Iterable[dict]:
    if source_config.api_query_parameters:
        api_query_parameters_config = source_config.api_query_parameters
        dict_value_list_from_bq = list(iter_dict_from_bq_query_for_bigquery_source_config(
            api_query_parameters_config.parameter_values.from_bigquery
        ))
        LOGGER.debug("dict_value_list_from_bq: %r", dict_value_list_from_bq)
        for dict_value_from_bq in dict_value_list_from_bq:
            entity_id_from_bq = dict_value_from_bq['entity_id']
            start_date_str_from_bq = dict_value_from_bq['start_date']
            campaign_start_date_str_from_bq = (
                dict_value_from_bq['campaign_start_date']
                if 'campaign_start_date' in dict_value_from_bq.keys()
                else start_date_str_from_bq
            )
            final_end_date_value = get_current_final_end_date(
                api_query_parameters_config=api_query_parameters_config,
                initial_start_date=date.fromisoformat(campaign_start_date_str_from_bq)
            )
            initial_start_date_value = date.fromisoformat(start_date_str_from_bq)
            placement_value_list = api_query_parameters_config.parameter_values.placement_value
            period_batch_size_in_days = (
                api_query_parameters_config.parameter_values.period_batch_size_in_days
            )
            if not api_query_parameters_config.parameter_names_for.placement:
                params_dict = get_param_dict_from_api_query_parameters(
                    api_query_parameters_config=api_query_parameters_config,
                    entity_id=entity_id_from_bq,
                    start_date=initial_start_date_value.isoformat(),
                    end_date=final_end_date_value.isoformat()
                )
                yield get_bq_compatible_json_response_from_resource_with_provenance(
                    source_config=source_config,
                    params_dict=params_dict
                )
            else:
                assert placement_value_list
                assert period_batch_size_in_days
                for placement_value in placement_value_list:
                    start_date_value = initial_start_date_value
                    LOGGER.debug('start_date_value: %r', start_date_value)
                    LOGGER.debug('final_end_date_value: %r', final_end_date_value)
                    while start_date_value < final_end_date_value:
                        end_date_value = get_end_date_value_of_batch_period(
                            start_date=start_date_value,
                            final_end_date=final_end_date_value,
                            # 7 days period is the max value for the api endpoint
                            batch_size_in_days=period_batch_size_in_days
                        )
                        params_dict = get_param_dict_from_api_query_parameters(
                            api_query_parameters_config=api_query_parameters_config,
                            entity_id=entity_id_from_bq,
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
        yield get_bq_compatible_json_response_from_resource_with_provenance(
                source_config=source_config,
                params_dict=None
            )


def fetch_twitter_ads_api_data_and_load_into_bq(
    config: TwitterAdsApiConfig
):
    batch_size = config.batch_size
    iterable_data_from_twitter_ads_api = (
        iter_bq_compatible_json_response_from_resource_with_provenance(
            source_config=config.source
        )
    )
    for batch_data_iterable in iter_batches_iterable(
        iterable_data_from_twitter_ads_api,
        batch_size
    ):
        batch_data_list = list(batch_data_iterable)
        LOGGER.info('loading batch into bigquery: %d', len(batch_data_list))
        load_given_json_list_data_from_tempdir_to_bq(
            project_name=config.target.project_name,
            dataset_name=config.target.dataset_name,
            table_name=config.target.table_name,
            json_list=batch_data_list
        )


def fetch_twitter_ads_api_data_and_load_into_bq_from_config_list(
    config_list: Sequence[TwitterAdsApiConfig]
):
    for config in config_list:
        fetch_twitter_ads_api_data_and_load_into_bq(config)
