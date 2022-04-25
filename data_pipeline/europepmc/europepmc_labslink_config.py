import dataclasses
import logging
from dataclasses import dataclass, field
from typing import NamedTuple

from data_pipeline.utils.pipeline_config import (
    BigQuerySourceConfig,
    get_resolved_parameter_values_from_file_path_env_name
)


LOGGER = logging.getLogger(__name__)


DEFAULT_LINKS_XML_FTP_FILENAME = 'links.xml'


class EuropePmcLabsLinkSourceConfig(NamedTuple):
    bigquery: BigQuerySourceConfig

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'EuropePmcLabsLinkSourceConfig':
        return EuropePmcLabsLinkSourceConfig(
            bigquery=BigQuerySourceConfig.from_dict(
                source_config_dict['bigQuery']
            )
        )


class EuropePmcLabsLinkXmlConfig(NamedTuple):
    provider_id: str
    link_title: str
    link_prefix: str

    @staticmethod
    def from_dict(xml_config_dict: dict) -> 'EuropePmcLabsLinkXmlConfig':
        return EuropePmcLabsLinkXmlConfig(
            provider_id=xml_config_dict['providerId'],
            link_title=xml_config_dict['linkTitle'],
            link_prefix=xml_config_dict['linkPrefix']
        )


@dataclass(frozen=True)
class FtpTargetConfig:
    hostname: str
    port: int
    username: str
    password: str = field(repr=False)
    directory_name: str = field(repr=False)
    create_directory: bool = False
    links_xml_filename: str = DEFAULT_LINKS_XML_FTP_FILENAME

    @staticmethod
    def from_dict(ftp_target_config_dict: dict) -> 'FtpTargetConfig':
        secrets = get_resolved_parameter_values_from_file_path_env_name(
            ftp_target_config_dict['parametersFromFile']
        )
        return FtpTargetConfig(
            hostname=ftp_target_config_dict['hostname'],
            port=ftp_target_config_dict['port'],
            username=ftp_target_config_dict['username'],
            password=secrets['password'],
            directory_name=secrets['directoryName'],
            create_directory=ftp_target_config_dict.get('createDirectory', False),
            links_xml_filename=ftp_target_config_dict.get(
                'linksXmlFilename',
                DEFAULT_LINKS_XML_FTP_FILENAME
            )
        )

    def _replace(self, **changes) -> 'FtpTargetConfig':
        """_replace provides same functionality of NamedTuple._replace"""
        return dataclasses.replace(self, **changes)


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
    xml: EuropePmcLabsLinkXmlConfig
    target: EuropePmcLabsLinkTargetConfig

    @staticmethod
    def _from_item_dict(item_config_dict: dict) -> 'EuropePmcLabsLinkConfig':
        return EuropePmcLabsLinkConfig(
            source=EuropePmcLabsLinkSourceConfig.from_dict(
                item_config_dict['source']
            ),
            xml=EuropePmcLabsLinkXmlConfig.from_dict(
                item_config_dict['xml']
            ),
            target=EuropePmcLabsLinkTargetConfig.from_dict(
                item_config_dict['target']
            )
        )

    @staticmethod
    def from_dict(config_dict: dict) -> 'EuropePmcLabsLinkConfig':
        LOGGER.debug('config_dict: %r', config_dict)
        item_config_list = config_dict['europePmcLabsLink']
        return EuropePmcLabsLinkConfig._from_item_dict(
            item_config_list[0]
        )
