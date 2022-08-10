
from typing import Iterable, NamedTuple, Sequence

from data_pipeline.utils.pipeline_config import (
    BigQuerySourceConfig,
    BigQueryTargetConfig,
    MappingConfig
)


class TwitterAdsApiSourceConfig(NamedTuple):
    resource: str
    secrets: MappingConfig = MappingConfig.from_dict({})
    param_from_bigquery: BigQuerySourceConfig = {}
    param_names: Iterable[str] = []

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'TwitterAdsApiSourceConfig':
        return TwitterAdsApiSourceConfig(
            resource=source_config_dict['resource'],
            secrets=MappingConfig.from_dict(source_config_dict['secrets']),
            param_from_bigquery=BigQuerySourceConfig.from_dict(
                source_config_dict.get('paramFromBigQuery', {})
            ),
            param_names=source_config_dict.get('paramNames', []),
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
