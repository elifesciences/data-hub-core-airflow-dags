from typing import NamedTuple, Sequence

from data_pipeline.utils.pipeline_config import (
    BigQuerySourceConfig,
    BigQueryTargetConfig,
    MappingConfig
)


class TwitterAdsApiParameterValuesConfig(NamedTuple):
    from_bigquery: BigQuerySourceConfig = {}
    start_time_value: str = ''

    @staticmethod
    def from_dict(parameter_values_config_dict: dict) -> 'TwitterAdsApiParameterValuesConfig':
        return TwitterAdsApiParameterValuesConfig(
            from_bigquery=BigQuerySourceConfig.from_dict(
                parameter_values_config_dict.get('fromBigQuery', {})
            ),
            start_time_value=parameter_values_config_dict.get('startTimeValue', '')
        )


class TwitterAdsApiParameterNamesForConfig(NamedTuple):
    bigquery_value: str = ''
    start_time: str = ''
    end_time: str = ''

    @staticmethod
    def from_dict(parameter_names_for_config_dict: dict) -> 'TwitterAdsApiParameterNamesForConfig':
        return TwitterAdsApiParameterNamesForConfig(
            bigquery_value=parameter_names_for_config_dict.get('bigqueryValue', ''),
            start_time=parameter_names_for_config_dict.get('startTime', ''),
            end_time=parameter_names_for_config_dict.get('endTime', '')
        )


class TwitterAdsApiApiQueryParametersConfig(NamedTuple):
    parameter_values: TwitterAdsApiParameterValuesConfig = {}
    parameter_names_for: TwitterAdsApiParameterNamesForConfig = {}

    @staticmethod
    def from_dict(api_query_parameters_config: dict) -> 'TwitterAdsApiApiQueryParametersConfig':
        return TwitterAdsApiApiQueryParametersConfig(
            parameter_values=TwitterAdsApiParameterValuesConfig.from_dict(
                api_query_parameters_config.get('parameterValues', {})
            ),
            parameter_names_for=TwitterAdsApiParameterNamesForConfig.from_dict(
                api_query_parameters_config.get('parameterNamesFor', {})
            )
        )


class TwitterAdsApiSourceConfig(NamedTuple):
    resource: str
    secrets: MappingConfig = MappingConfig.from_dict({})
    api_query_parameters: TwitterAdsApiApiQueryParametersConfig = {}

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'TwitterAdsApiSourceConfig':
        return TwitterAdsApiSourceConfig(
            resource=source_config_dict['resource'],
            secrets=MappingConfig.from_dict(source_config_dict['secrets']),
            api_query_parameters=TwitterAdsApiApiQueryParametersConfig.from_dict(
                source_config_dict.get('apiQueryParameters', {})
            )
        )


class TwitterAdsApiConfig(NamedTuple):
    source: TwitterAdsApiSourceConfig
    target: BigQueryTargetConfig

    @staticmethod
    def _from_item_dict(item_config_dict) -> 'TwitterAdsApiConfig':
        return TwitterAdsApiConfig(
            source=TwitterAdsApiSourceConfig.from_dict(
                item_config_dict['source']
            ),
            target=BigQueryTargetConfig.from_dict(
                item_config_dict['target']
            )
        )

    @staticmethod
    def parse_config_list_from_dict(config_dict: dict) -> Sequence['TwitterAdsApiConfig']:
        item_config_dict_list = config_dict['twitterAdsApi']
        return [
            TwitterAdsApiConfig._from_item_dict(item_config_dict)
            for item_config_dict in item_config_dict_list
        ]
