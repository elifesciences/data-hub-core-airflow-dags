from datetime import datetime
from pathlib import Path

import pytest

from data_pipeline.opensearch.bigquery_to_opensearch_config import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_OPENSEARCH_TIMEOUT,
    BigQueryToOpenSearchConfig,
    OpenSearchIngestionPipelineConfig,
    OpenSearchOperationModes,
    OpenSearchTargetConfig
)
from data_pipeline.opensearch.bigquery_to_opensearch_config_typing import (
    OpenSearchIngestionPipelineConfigDict,
    OpenSearchTargetConfigDict
)
from data_pipeline.utils.pipeline_config import BigQuerySourceConfig


USERNAME_1 = 'username1'
PASSWORD_1 = 'password1'


ID_FIELD_NAME = 'id1'
TIMESTAMP_FIELD_NAME = 'timestamp1'


OPENSEARCH_USERNAME_FILE_PATH_ENV_VAR = 'OPENSEARCH_USERNAME_FILE_PATH_ENV_VAR'
OPENSEARCH_PASSWORD_FILE_PATH_ENV_VAR = 'OPENSEARCH_PASSWORD_FILE_PATH_ENV_VAR'


INITIAL_START_TIMESTAMP_STR_1 = '2001-02-03+00:00'

BUCKET_NAME_1 = 'bucket1'
OBJECT_NAME_1 = 'object1'

STATE_CONFIG_DICT_1 = {
    'initialState': {
        'startTimestamp': INITIAL_START_TIMESTAMP_STR_1
    },
    'stateFile': {
        'bucketName': BUCKET_NAME_1,
        'objectName': OBJECT_NAME_1
    }
}


BIGQUERY_SOURCE_CONFIG_DICT_1 = {
    'projectName': 'project1',
    'sqlQuery': 'query1'
}


OPENSEARCH_INDEX_SETTNGS_1 = {
    'settings': {'index': {'some_setting': 'value'}}
}


OPENSEARCH_INGESTION_PIPELINE_CONFIG_DICT_1: OpenSearchIngestionPipelineConfigDict = {
    'name': 'ingestion_pipeline_1',
    'definition': 'ingestion_pipeline_definition_1'
}


OPENSEARCH_TARGET_CONFIG_DICT_1: OpenSearchTargetConfigDict = {
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
    'indexName': 'index_1'
}


CONFIG_DICT_1: dict = {
    'dataPipelineId': 'pipeline_1',
    'source': {'bigQuery': BIGQUERY_SOURCE_CONFIG_DICT_1},
    'fieldNamesFor': {
        'id': ID_FIELD_NAME,
        'timestamp': TIMESTAMP_FIELD_NAME
    },
    'target': {'openSearch': OPENSEARCH_TARGET_CONFIG_DICT_1},
    'state': STATE_CONFIG_DICT_1
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


class TestOpenSearchIngestionPipelineConfig:
    def test_should_read_name_and_definition(self):
        ingestion_pipeline_config = OpenSearchIngestionPipelineConfig.from_dict(
            OPENSEARCH_INGESTION_PIPELINE_CONFIG_DICT_1
        )
        assert ingestion_pipeline_config.name == (
            OPENSEARCH_INGESTION_PIPELINE_CONFIG_DICT_1['name']
        )
        assert ingestion_pipeline_config.definition == (
            OPENSEARCH_INGESTION_PIPELINE_CONFIG_DICT_1['definition']
        )


class TestOpenSearchTargetConfig:
    def test_should_read_hostname_and_port(self):
        opensearch_target_config = OpenSearchTargetConfig.from_dict(
            OPENSEARCH_TARGET_CONFIG_DICT_1
        )
        assert opensearch_target_config.hostname == OPENSEARCH_TARGET_CONFIG_DICT_1['hostname']
        assert opensearch_target_config.port == OPENSEARCH_TARGET_CONFIG_DICT_1['port']

    def test_should_use_default_timeout(self):
        assert 'timeout' not in OPENSEARCH_TARGET_CONFIG_DICT_1
        opensearch_target_config = OpenSearchTargetConfig.from_dict(
            OPENSEARCH_TARGET_CONFIG_DICT_1
        )
        assert opensearch_target_config.timeout == DEFAULT_OPENSEARCH_TIMEOUT

    def test_should_read_timeout(self):
        opensearch_target_config = OpenSearchTargetConfig.from_dict({
            **OPENSEARCH_TARGET_CONFIG_DICT_1,
            'timeout': 0.123
        })
        assert opensearch_target_config.timeout == 0.123

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
        assert opensearch_target_config.index_name == OPENSEARCH_TARGET_CONFIG_DICT_1['indexName']

    def test_should_allow_no_index_settings(self):
        assert 'indexSettings' not in OPENSEARCH_TARGET_CONFIG_DICT_1
        opensearch_target_config = OpenSearchTargetConfig.from_dict(
            OPENSEARCH_TARGET_CONFIG_DICT_1,
        )
        assert opensearch_target_config.index_settings is None

    def test_should_read_index_settings_if_configured(self):
        opensearch_target_config = OpenSearchTargetConfig.from_dict({
            **OPENSEARCH_TARGET_CONFIG_DICT_1,
            'indexSettings': OPENSEARCH_INDEX_SETTNGS_1
        })
        assert opensearch_target_config.index_settings == OPENSEARCH_INDEX_SETTNGS_1

    @pytest.mark.parametrize('value', [False,  True])
    def test_should_read_update_index_settings(self, value: bool):
        opensearch_target_config = OpenSearchTargetConfig.from_dict({
            **OPENSEARCH_TARGET_CONFIG_DICT_1,
            'updateIndexSettings': value
        })
        assert opensearch_target_config.update_index_settings == value

    @pytest.mark.parametrize('value', [False,  True])
    def test_should_read_update_mappings(self, value: bool):
        opensearch_target_config = OpenSearchTargetConfig.from_dict({
            **OPENSEARCH_TARGET_CONFIG_DICT_1,
            'updateMappings': value
        })
        assert opensearch_target_config.update_mappings == value

    def test_should_verify_certificates_by_default(self):
        opensearch_target_config = OpenSearchTargetConfig.from_dict(
            OPENSEARCH_TARGET_CONFIG_DICT_1
        )
        assert opensearch_target_config.verify_certificates is True

    def test_should_read_verify_certificates_false(self):
        opensearch_target_config = OpenSearchTargetConfig.from_dict({
            **OPENSEARCH_TARGET_CONFIG_DICT_1,
            'verifyCertificates': False
        })
        assert opensearch_target_config.verify_certificates is False

    @pytest.mark.parametrize('value', [
        OpenSearchOperationModes.INDEX, OpenSearchOperationModes.UPDATE
    ])
    def test_should_read_operation_mode(self, value: str):
        opensearch_target_config = OpenSearchTargetConfig.from_dict({
            **OPENSEARCH_TARGET_CONFIG_DICT_1,
            'operationMode': value
        })
        assert opensearch_target_config.operation_mode == value

    def test_should_reject_invalid_operation_mode(self):
        with pytest.raises(ValueError):
            OpenSearchTargetConfig.from_dict({
                **OPENSEARCH_TARGET_CONFIG_DICT_1,
                'operationMode': 'invalid'
            })

    @pytest.mark.parametrize('value', [False,  True])
    def test_should_read_upsert(self, value: bool):
        opensearch_target_config = OpenSearchTargetConfig.from_dict({
            **OPENSEARCH_TARGET_CONFIG_DICT_1,
            'upsert': value
        })
        assert opensearch_target_config.upsert == value

    def test_should_read_ingestion_pipelines(self):
        opensearch_target_config = OpenSearchTargetConfig.from_dict({
            **OPENSEARCH_TARGET_CONFIG_DICT_1,
            'ingestionPipelines': [OPENSEARCH_INGESTION_PIPELINE_CONFIG_DICT_1]
        })
        assert opensearch_target_config.ingestion_pipelines == [
            OpenSearchIngestionPipelineConfig.from_dict(
                OPENSEARCH_INGESTION_PIPELINE_CONFIG_DICT_1
            )
        ]


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

    def test_should_read_data_pipeline_id(self):
        config_list = list(BigQueryToOpenSearchConfig.parse_config_list_from_dict({
            'bigQueryToOpenSearch': [CONFIG_DICT_1]
        }))
        assert (
            config_list[0].data_pipeline_id == CONFIG_DICT_1['dataPipelineId']
        )

    def test_should_read_bigquery_source_config(self):
        config_list = list(BigQueryToOpenSearchConfig.parse_config_list_from_dict({
            'bigQueryToOpenSearch': [CONFIG_DICT_1]
        }))
        assert (
            config_list[0].source.bigquery
            == BigQuerySourceConfig.from_dict(BIGQUERY_SOURCE_CONFIG_DICT_1)
        )

    def test_should_read_field_names_for_config(self):
        config_list = list(BigQueryToOpenSearchConfig.parse_config_list_from_dict({
            'bigQueryToOpenSearch': [CONFIG_DICT_1]
        }))
        assert config_list[0].field_names_for.id_key_path == [ID_FIELD_NAME]
        assert config_list[0].field_names_for.timestamp_key_path == [TIMESTAMP_FIELD_NAME]

    def test_should_use_default_batch_size_for_config(self):
        assert 'batchSize' not in CONFIG_DICT_1
        config_list = list(BigQueryToOpenSearchConfig.parse_config_list_from_dict({
            'bigQueryToOpenSearch': [CONFIG_DICT_1]
        }))
        assert config_list[0].batch_size == DEFAULT_BATCH_SIZE

    def test_should_read_batch_size_for_config(self):
        config_list = list(BigQueryToOpenSearchConfig.parse_config_list_from_dict({
            'bigQueryToOpenSearch': [{
                **CONFIG_DICT_1,
                'batchSize': 123
            }]
        }))
        assert config_list[0].batch_size == 123

    def test_should_read_initial_state_start_date(self):
        config_list = list(BigQueryToOpenSearchConfig.parse_config_list_from_dict({
            'bigQueryToOpenSearch': [CONFIG_DICT_1]
        }))
        assert (
            config_list[0].state.initial_state.start_timestamp
            == datetime.fromisoformat(INITIAL_START_TIMESTAMP_STR_1)
        )

    def test_should_read_state_file_config(self):
        config_list = list(BigQueryToOpenSearchConfig.parse_config_list_from_dict({
            'bigQueryToOpenSearch': [CONFIG_DICT_1]
        }))
        assert config_list[0].state.state_file.bucket_name == BUCKET_NAME_1
        assert config_list[0].state.state_file.object_name == OBJECT_NAME_1

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
