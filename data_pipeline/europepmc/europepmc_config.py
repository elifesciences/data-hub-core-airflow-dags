from typing import NamedTuple, Sequence


class EuropePmcSearchConfig(NamedTuple):
    query: str

    @staticmethod
    def from_dict(search_config_dict: dict) -> 'EuropePmcSearchConfig':
        return EuropePmcSearchConfig(
            query=search_config_dict['query']
        )


class EuropePmcSourceConfig(NamedTuple):
    search: EuropePmcSearchConfig
    fields_to_return: Sequence[str]

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'EuropePmcSourceConfig':
        return EuropePmcSourceConfig(
            search=EuropePmcSearchConfig.from_dict(
                source_config_dict['search']
            ),
            fields_to_return=source_config_dict['fieldsToReturn']
        )


class EuropePmcConfig(NamedTuple):
    source: EuropePmcSourceConfig

    @staticmethod
    def _from_item_dict(item_config_dict: dict) -> 'EuropePmcConfig':
        return EuropePmcConfig(
            source=EuropePmcSourceConfig.from_dict(
                item_config_dict['source']
            )
        )

    @staticmethod
    def from_dict(config_dict: dict) -> 'EuropePmcConfig':
        item_config_list = config_dict['europePmc']
        return EuropePmcConfig._from_item_dict(
            item_config_list[0]
        )
