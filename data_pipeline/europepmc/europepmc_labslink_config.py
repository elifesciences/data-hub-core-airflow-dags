from dataclasses import dataclass, field
from typing import NamedTuple

from data_pipeline.generic_web_api.url_builder import (
    compose_url_param_from_param_vals_filepath_in_env_var
)


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


@dataclass(frozen=True)
class FtpTargetConfig:
    host: str
    username: str
    password: str = field(repr=False)
    directory_name: str = field(repr=False)

    @staticmethod
    def from_dict(ftp_target_config_dict: dict) -> 'FtpTargetConfig':
        secrets = compose_url_param_from_param_vals_filepath_in_env_var(
            ftp_target_config_dict['parametersFromFile']
        )
        return FtpTargetConfig(
            host=ftp_target_config_dict['host'],
            username=ftp_target_config_dict['username'],
            password=secrets['password'],
            directory_name=secrets['directoryName']
        )


class EuropePmcLabsLinkTargetConfig(NamedTuple):
    ftp: FtpTargetConfig

    @staticmethod
    def from_dict(target_config_dict: dict) -> 'EuropePmcLabsLinkTargetConfig':
        return EuropePmcLabsLinkTargetConfig(
            ftp=FtpTargetConfig.from_dict(
                target_config_dict['ftp']
            )
        )


class EuropePmcLabsLinkConfig(NamedTuple):
    source: EuropePmcLabsLinkSourceConfig
    target: EuropePmcLabsLinkTargetConfig

    @staticmethod
    def _from_item_dict(item_config_dict: dict) -> 'EuropePmcLabsLinkConfig':
        return EuropePmcLabsLinkConfig(
            source=EuropePmcLabsLinkSourceConfig.from_dict(
                item_config_dict['source']
            ),
            target=EuropePmcLabsLinkTargetConfig.from_dict(
                item_config_dict['target']
            )
        )

    @staticmethod
    def from_dict(config_dict: dict) -> 'EuropePmcLabsLinkConfig':
        item_config_list = config_dict['europePmcLabsLink']
        return EuropePmcLabsLinkConfig._from_item_dict(
            item_config_list[0]
        )
