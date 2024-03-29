from typing import NamedTuple, Optional, Sequence

from data_pipeline.utils.pipeline_config import (
    BigQuerySourceConfig,
    BigQueryTargetConfig,
    MappingConfig
)

DEFAULT_BATCH_SIZE = 1000


class TwitterAdsApiParameterValuesConfig(NamedTuple):
    from_bigquery: BigQuerySourceConfig
    max_period_in_days: int = 0
    placement_value: Optional[Sequence[str]] = None
    period_batch_size_in_days: Optional[int] = 0

    @staticmethod
    def from_dict(parameter_values_config_dict: dict) -> 'TwitterAdsApiParameterValuesConfig':
        return TwitterAdsApiParameterValuesConfig(
            from_bigquery=BigQuerySourceConfig.from_dict(
                parameter_values_config_dict['fromBigQuery']
            ),
            max_period_in_days=parameter_values_config_dict.get('maxPeriodInDays', 0),
            period_batch_size_in_days=parameter_values_config_dict.get('periodBatchSizeInDays', 0),
            placement_value=parameter_values_config_dict.get('placementValue', [])
        )


class TwitterAdsApiParameterNamesForConfig(NamedTuple):
    entity_id: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    placement: Optional[str] = None

    @staticmethod
    def from_dict(parameter_names_for_config_dict: dict) -> 'TwitterAdsApiParameterNamesForConfig':
        return TwitterAdsApiParameterNamesForConfig(
            entity_id=parameter_names_for_config_dict.get('entityId', None),
            start_date=parameter_names_for_config_dict.get('startDate', None),
            end_date=parameter_names_for_config_dict.get('endDate', None),
            placement=parameter_names_for_config_dict.get('placement', None),
        )


class TwitterAdsApiApiQueryParametersConfig(NamedTuple):
    parameter_values: TwitterAdsApiParameterValuesConfig
    parameter_names_for: TwitterAdsApiParameterNamesForConfig

    @staticmethod
    def from_dict(api_query_parameters_config: dict) -> 'TwitterAdsApiApiQueryParametersConfig':
        return TwitterAdsApiApiQueryParametersConfig(
            parameter_values=TwitterAdsApiParameterValuesConfig.from_dict(
                api_query_parameters_config['parameterValues']
            ),
            parameter_names_for=TwitterAdsApiParameterNamesForConfig.from_dict(
                api_query_parameters_config['parameterNamesFor']
            )
        )


class TwitterAdsApiSourceConfig(NamedTuple):
    resource: str
    secrets: MappingConfig = MappingConfig.from_dict({})
    api_query_parameters: Optional[TwitterAdsApiApiQueryParametersConfig] = None
    account_ids: Optional[Sequence[str]] = None

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'TwitterAdsApiSourceConfig':
        if source_config_dict.get('apiQueryParameters'):
            return TwitterAdsApiSourceConfig(
                account_ids=source_config_dict.get('accountIds'),
                resource=source_config_dict['resource'],
                secrets=MappingConfig.from_dict(source_config_dict['secrets']),
                api_query_parameters=TwitterAdsApiApiQueryParametersConfig.from_dict(
                    source_config_dict.get('apiQueryParameters', {})
                )
            )
        return TwitterAdsApiSourceConfig(
                account_ids=source_config_dict.get('accountIds'),
                resource=source_config_dict['resource'],
                secrets=MappingConfig.from_dict(source_config_dict['secrets'])
            )


class TwitterAdsApiConfig(NamedTuple):
    source: TwitterAdsApiSourceConfig
    target: BigQueryTargetConfig
    batch_size: int = DEFAULT_BATCH_SIZE

    @staticmethod
    def _from_item_dict(item_config_dict) -> 'TwitterAdsApiConfig':
        return TwitterAdsApiConfig(
            source=TwitterAdsApiSourceConfig.from_dict(
                item_config_dict['source']
            ),
            target=BigQueryTargetConfig.from_dict(
                item_config_dict['target']
            ),
            batch_size=item_config_dict.get('batchSize', DEFAULT_BATCH_SIZE)
        )

    @staticmethod
    def parse_config_list_from_dict(config_dict: dict) -> Sequence['TwitterAdsApiConfig']:
        item_config_dict_list = config_dict['twitterAdsApi']
        return [
            TwitterAdsApiConfig._from_item_dict(item_config_dict)
            for item_config_dict in item_config_dict_list
        ]
