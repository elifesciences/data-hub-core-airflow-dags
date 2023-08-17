import logging
from dataclasses import dataclass
from typing import Sequence


LOGGER = logging.getLogger(__name__)


@dataclass
class BigQueryToOpenSearchConfig:

    @staticmethod
    def parse_config_list_from_dict(config_dict: dict) -> Sequence['BigQueryToOpenSearchConfig']:
        LOGGER.debug('config_dict: %r', config_dict)
        return []
