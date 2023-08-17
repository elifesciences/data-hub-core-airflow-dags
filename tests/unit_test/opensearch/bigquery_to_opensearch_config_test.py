from pathlib import Path

import pytest

from data_pipeline.opensearch.bigquery_to_opensearch_config import (
    BigQueryToOpenSearchConfig,
    OpenSearchTargetConfig
)
from data_pipeline.utils.pipeline_config import BigQuerySourceConfig


USERNAME_1 = 'username1'
PASSWORD_1 = 'password1'


OPENSEARCH_USERNAME_FILE_PATH_ENV_VAR = 'OPENSEARCH_USERNAME_FILE_PATH_ENV_VAR'
OPENSEARCH_PASSWORD_FILE_PATH_ENV_VAR = 'OPENSEARCH_PASSWORD_FILE_PATH_ENV_VAR'


BIGQUERY_SOURCE_CONFIG_DICT_1 = {
    'projectName': 'project1',
    'sqlQuery': 'query1'
}


OPENSEARCH_TARGET_CONFIG_DICT_1 = {
    'hostname': 'hostname1',
    'port': 9200,
    'secrets': {
        'parametersFromFile': [{
            'parameterName': 'username',
            'filePathEnvName': OPENSEARCH_USERNAME_FILE_PATH_ENV_VAR
        }, {
            'parameterName': 'password',
            'filePathEnvName': OPENSEARCH_PASSWORD_FILE_PATH_ENV_VAR
        }]
    },
    'index_name': 'index_1'
}


CONFIG_DICT_1: dict = {
    'source': {'bigQuery': BIGQUERY_SOURCE_CONFIG_DICT_1},
    'target': {'opensearch': OPENSEARCH_TARGET_CONFIG_DICT_1}
}


@pytest.fixture(name='username_file_path', autouse=True)
def _username_file_path(mock_env: dict, tmp_path: Path) -> str:
    file_path = tmp_path / 'username'
    file_path.write_text(USERNAME_1)
    mock_env[OPENSEARCH_USERNAME_FILE_PATH_ENV_VAR] = str(file_path)
    return str(file_path)


@pytest.fixture(name='password_file_path', autouse=True)
def _password_file_path(mock_env: dict, tmp_path: Path) -> str:
    file_path = tmp_path / 'password'
    file_path.write_text(PASSWORD_1)
    mock_env[OPENSEARCH_PASSWORD_FILE_PATH_ENV_VAR] = str(file_path)
    return str(file_path)


class TestOpenSearchTargetConfig:
    def test_should_read_hostname_and_port(self):
        opensearch_target_config = OpenSearchTargetConfig.from_dict(
            OPENSEARCH_TARGET_CONFIG_DICT_1
        )
        assert opensearch_target_config.hostname == OPENSEARCH_TARGET_CONFIG_DICT_1['hostname']
        assert opensearch_target_config.port == OPENSEARCH_TARGET_CONFIG_DICT_1['port']

    def test_should_read_target_username_and_password_from_file_path_env_name(
        self
    ):
        opensearch_target_config = OpenSearchTargetConfig.from_dict(
            OPENSEARCH_TARGET_CONFIG_DICT_1
        )
        assert opensearch_target_config.username == USERNAME_1
        assert opensearch_target_config.password == PASSWORD_1

    def test_should_read_index_name(self):
        opensearch_target_config = OpenSearchTargetConfig.from_dict(
            OPENSEARCH_TARGET_CONFIG_DICT_1
        )
        assert opensearch_target_config.index_name == OPENSEARCH_TARGET_CONFIG_DICT_1['index_name']


class TestBigQueryToOpenSearchConfig:
    def test_should_load_empty_list_with_empty_config(self):
        config_list = list(BigQueryToOpenSearchConfig.parse_config_list_from_dict({
            'bigQueryToOpenSearch': []
        }))
        assert not config_list

    def test_should_load_single_entry_entry_config(self):
        config_list = list(BigQueryToOpenSearchConfig.parse_config_list_from_dict({
            'bigQueryToOpenSearch': [CONFIG_DICT_1]
        }))
        assert len(config_list) == 1

    def test_should_read_bigquery_source_config(self):
        config_list = list(BigQueryToOpenSearchConfig.parse_config_list_from_dict({
            'bigQueryToOpenSearch': [CONFIG_DICT_1]
        }))
        assert (
            config_list[0].source.bigquery
            == BigQuerySourceConfig.from_dict(BIGQUERY_SOURCE_CONFIG_DICT_1)
        )

    def test_should_read_opensearch_target_config(self):
        config_list = list(BigQueryToOpenSearchConfig.parse_config_list_from_dict({
            'bigQueryToOpenSearch': [CONFIG_DICT_1]
        }))
        assert (
            config_list[0].target.opensearch
            == OpenSearchTargetConfig.from_dict(OPENSEARCH_TARGET_CONFIG_DICT_1)
        )

    def test_should_not_include_secrets_in_repr_or_str_output(
        self
    ):
        config = BigQueryToOpenSearchConfig.parse_config_list_from_dict({
            'bigQueryToOpenSearch': [CONFIG_DICT_1]
        })
        text = f'repr={repr(config)}, str={str(config)}'
        assert USERNAME_1 not in text
        assert PASSWORD_1 not in text
