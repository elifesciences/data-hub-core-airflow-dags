
from typing import Iterable, NamedTuple

from data_pipeline.utils.pipeline_config import (
    BigQuerySourceConfig,
    BigQueryTargetConfig,
    MappingConfig
)


class TwitterAdsApiSourceConfig(NamedTuple):
    resource: str
    secrets: MappingConfig = MappingConfig.from_dict({})
    bigquery: BigQuerySourceConfig = {}
    param_names: Iterable[str] = []

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'TwitterAdsApiSourceConfig':
        return TwitterAdsApiSourceConfig(
            resource=source_config_dict['resource'],
            secrets=MappingConfig.from_dict(source_config_dict['secrets']),
            param_names=source_config_dict.get('paramNames', []),
            bigquery=BigQuerySourceConfig.from_dict(source_config_dict.get('bigQuery', {}))
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
    def from_dict(config_dict: dict) -> 'TwitterAdsApiConfig':
        item_config_list = config_dict['twitterAdsApi']
        return TwitterAdsApiConfig._from_item_dict(
            item_config_list[0]
        )
