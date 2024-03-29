from pathlib import Path

import pytest

from data_pipeline.europepmc.europepmc_labslink_config import (
    DEFAULT_LINKS_XML_FTP_FILENAME,
    EuropePmcLabsLinkConfig
)


SQL_QUERY_1 = 'query 1'
PROJECT_NAME_1 = 'project name1'


BIGQUERY_SOURCE_CONFIG_DICT_1 = {
    'projectName': PROJECT_NAME_1,
    'sqlQuery': SQL_QUERY_1
}

SOURCE_CONFIG_DICT_1 = {
    'bigQuery': BIGQUERY_SOURCE_CONFIG_DICT_1
}

PASSWORD_1 = 'password1'
DIRECTORY_NAME_1 = 'directory name1'
LINKS_XML_FILENAME_1 = 'links1.xml'

FTP_PASSWORD_FILE_PATH_ENV_VAR = 'FTP_PASSWORD_FILE_PATH_ENV_VAR'
FTP_DIRECTORY_NAME_FILE_PATH_ENV_VAR = 'FTP_DIRECTORY_NAME_FILE_PATH_ENV_VAR'

PROVIDER_ID_1 = '1234'
LINK_TITLE_1 = 'Link Title 1'
LINK_PREFIX_1 = 'https://link-prefix/'

XML_CONFIG_DICT_1 = {
    'providerId': PROVIDER_ID_1,
    'linkTitle': LINK_TITLE_1,
    'linkPrefix': LINK_PREFIX_1
}

FTP_TARGET_CONFIG_DICT_1 = {
    'hostname': 'hostname1',
    'port': 123,
    'username': 'username1',
    'parametersFromFile': [{
        'parameterName': 'password',
        'filePathEnvName': FTP_PASSWORD_FILE_PATH_ENV_VAR
    }, {
        'parameterName': 'directoryName',
        'filePathEnvName': FTP_DIRECTORY_NAME_FILE_PATH_ENV_VAR
    }]
}

TARGET_CONFIG_DICT_1 = {
    'ftp': FTP_TARGET_CONFIG_DICT_1
}

ITEM_CONFIG_DICT_1 = {
    'source': SOURCE_CONFIG_DICT_1,
    'xml': XML_CONFIG_DICT_1,
    'target': TARGET_CONFIG_DICT_1
}


def get_config_for_item_config_dict(item_dict: dict) -> dict:
    return {'europePmcLabsLink': [item_dict]}


CONFIG_DICT_1 = get_config_for_item_config_dict(ITEM_CONFIG_DICT_1)


@pytest.fixture(name='password_file_path', autouse=True)
def _password_file_path(mock_env: dict, tmp_path: Path) -> str:
    file_path = tmp_path / 'password'
    file_path.write_text(PASSWORD_1)
    mock_env[FTP_PASSWORD_FILE_PATH_ENV_VAR] = str(file_path)
    return str(file_path)


@pytest.fixture(name='directory_name_file_path', autouse=True)
def _directory_name_file_path(mock_env: dict, tmp_path: Path) -> str:
    file_path = tmp_path / 'directory_name'
    file_path.write_text(DIRECTORY_NAME_1)
    mock_env[FTP_DIRECTORY_NAME_FILE_PATH_ENV_VAR] = str(file_path)
    return str(file_path)


class TestEuropeLabsLinkPmcConfig:
    def test_should_read_bigquery_sql_query(self):
        config = EuropePmcLabsLinkConfig.from_dict(CONFIG_DICT_1)
        assert config.source.bigquery.sql_query == SQL_QUERY_1

    def test_should_read_bigquery_project_name(self):
        config = EuropePmcLabsLinkConfig.from_dict(CONFIG_DICT_1)
        assert config.source.bigquery.project_name == PROJECT_NAME_1

    def test_should_read_xml_provider_id_link_title_and_prefix(self):
        config = EuropePmcLabsLinkConfig.from_dict(CONFIG_DICT_1)
        assert config.xml.provider_id == PROVIDER_ID_1
        assert config.xml.link_title == LINK_TITLE_1
        assert config.xml.link_prefix == LINK_PREFIX_1

    def test_should_read_target_ftp_hostname_port_and_username(self):
        config = EuropePmcLabsLinkConfig.from_dict(CONFIG_DICT_1)
        assert config.target.ftp.hostname == FTP_TARGET_CONFIG_DICT_1['hostname']
        assert config.target.ftp.port == FTP_TARGET_CONFIG_DICT_1['port']
        assert config.target.ftp.username == FTP_TARGET_CONFIG_DICT_1['username']

    def test_should_use_default_ftp_links_xml_filename(self):
        config = EuropePmcLabsLinkConfig.from_dict(CONFIG_DICT_1)
        assert config.target.ftp.links_xml_filename == DEFAULT_LINKS_XML_FTP_FILENAME

    def test_should_read_target_ftp_links_xml_filename(self):
        config = EuropePmcLabsLinkConfig.from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT_1,
            'target': {'ftp': {
                **FTP_TARGET_CONFIG_DICT_1,
                'linksXmlFilename': LINKS_XML_FILENAME_1
            }}
        }))
        assert config.target.ftp.links_xml_filename == LINKS_XML_FILENAME_1

    def test_should_set_target_ftp_create_target_directory_to_false_by_default(self):
        config = EuropePmcLabsLinkConfig.from_dict(CONFIG_DICT_1)
        assert config.target.ftp.create_directory is False

    def test_should_be_able_to_set_target_ftp_create_target_directory_to_true(self):
        config = EuropePmcLabsLinkConfig.from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT_1,
            'target': {'ftp': {
                **FTP_TARGET_CONFIG_DICT_1,
                'createDirectory': True
            }}
        }))
        assert config.target.ftp.create_directory is True

    def test_should_read_target_ftp_password_and_directory_name_from_file_path_env_name(
        self
    ):
        config = EuropePmcLabsLinkConfig.from_dict(CONFIG_DICT_1)
        assert config.target.ftp.password == PASSWORD_1
        assert config.target.ftp.directory_name == DIRECTORY_NAME_1

    def test_should_not_include_secrets_in_repr_or_str_output(
        self
    ):
        config = EuropePmcLabsLinkConfig.from_dict(CONFIG_DICT_1)
        text = f'repr={repr(config)}, str={str(config)}'
        assert PASSWORD_1 not in text
        assert DIRECTORY_NAME_1 not in text
