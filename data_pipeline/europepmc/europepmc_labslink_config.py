from typing import NamedTuple


class BigQuerySourceConfig(NamedTuple):
    sql_query: str

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'BigQuerySourceConfig':
        return BigQuerySourceConfig(
            sql_query=source_config_dict['sqlQuery']
        )


class EuropePmcLabsLinkSourceConfig(NamedTuple):
    bigquery: BigQuerySourceConfig

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'EuropePmcLabsLinkSourceConfig':
        return EuropePmcLabsLinkSourceConfig(
            bigquery=BigQuerySourceConfig.from_dict(
                source_config_dict['bigQuery']
            )
        )


class EuropePmcLabsLinkConfig(NamedTuple):
    source: EuropePmcLabsLinkSourceConfig

    @staticmethod
    def _from_item_dict(item_config_dict: dict) -> 'EuropePmcLabsLinkConfig':
        return EuropePmcLabsLinkConfig(
            source=EuropePmcLabsLinkSourceConfig.from_dict(
                item_config_dict['source']
            )
        )

    @staticmethod
    def from_dict(config_dict: dict) -> 'EuropePmcLabsLinkConfig':
        item_config_list = config_dict['europePmcLabsLink']
        return EuropePmcLabsLinkConfig._from_item_dict(
            item_config_list[0]
        )
