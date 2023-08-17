import logging
from dataclasses import dataclass
from typing import Sequence


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class BigQueryToOpenSearchConfig:
    @staticmethod
    def _from_item_dict(
        item_config_dict: dict  # pylint: disable=unused-argument
    ) -> 'BigQueryToOpenSearchConfig':
        return BigQueryToOpenSearchConfig()

    @staticmethod
    def parse_config_list_from_dict(config_dict: dict) -> Sequence['BigQueryToOpenSearchConfig']:
        LOGGER.debug('config_dict: %r', config_dict)
        item_config_dict_list = config_dict['bigQueryToOpenSearch']
        return [
            BigQueryToOpenSearchConfig._from_item_dict(item_config_dict)
            for item_config_dict in item_config_dict_list
        ]
