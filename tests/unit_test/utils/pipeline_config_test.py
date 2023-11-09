import logging
from pathlib import Path

import pytest

from data_pipeline.utils.pipeline_config import (
    SECRET_VALUE_PLACEHOLDER,
    BigQueryIncludeExcludeSourceConfig,
    BigQuerySourceConfig,
    BigQueryTargetConfig,
    MappingConfig,
    StateFileConfig,
    get_resolved_parameter_values_from_file_path_env_name,
    parse_key_path,
    parse_required_non_empty_key_path,
    str_to_bool,
    get_environment_variable_value
)


LOGGER = logging.getLogger(__name__)


BIGQUERY_SOURCE_CONFIG_DICT_1 = {
    'projectName': 'project1',
    'sqlQuery': 'query1'
}

ENV_VAR_1 = 'env1'

KEY_1 = 'key1'
VALUE_1 = 'value1'
PARAMETERS_FROM_FILE_CONFIG_DICT_1 = {
    'parameterName': KEY_1,
    'filePathEnvName': ENV_VAR_1
}


@pytest.fixture(name='existing_secret_file_path_1')
def _existing_secret_file_path_1(tmp_path: Path) -> Path:
    value_file_path = tmp_path / 'secret1'
    value_file_path.write_text(VALUE_1)
    return value_file_path


@pytest.fixture(name='existing_secret_file_and_env_1')
def _existing_secret_file_and_env_1(
    mock_env: dict,
    existing_secret_file_path_1: Path
):
    mock_env[ENV_VAR_1] = str(existing_secret_file_path_1)


class TestBigQuerySourceConfig:
    def test_should_read_project_and_sql_query(self):
        config = BigQuerySourceConfig.from_dict(BIGQUERY_SOURCE_CONFIG_DICT_1)
        assert config.project_name == BIGQUERY_SOURCE_CONFIG_DICT_1['projectName']
        assert config.sql_query == BIGQUERY_SOURCE_CONFIG_DICT_1['sqlQuery']

    def test_should_default_ignore_not_found_to_false(self):
        config = BigQuerySourceConfig.from_dict(BIGQUERY_SOURCE_CONFIG_DICT_1)
        assert config.ignore_not_found is False

    def test_should_allow_to_set_ignore_not_found_to_true(self):
        config = BigQuerySourceConfig.from_dict({
            **BIGQUERY_SOURCE_CONFIG_DICT_1,
            'ignoreNotFound': True
        })
        assert config.ignore_not_found is True


class TestBigQueryIncludeExcludeSourceConfig:
    def test_should_read_matrix_variables_without_exclude(self):
        config = BigQueryIncludeExcludeSourceConfig.from_dict({
            'include': {
                'bigQuery': BIGQUERY_SOURCE_CONFIG_DICT_1
            }
        })
        assert config.include.bigquery.project_name == BIGQUERY_SOURCE_CONFIG_DICT_1['projectName']
        assert config.include.bigquery.sql_query == BIGQUERY_SOURCE_CONFIG_DICT_1['sqlQuery']
        assert not config.exclude

    def test_should_read_matrix_variables_with_exclude(self):
        config = BigQueryIncludeExcludeSourceConfig.from_dict({
            'include': {
                'bigQuery': BIGQUERY_SOURCE_CONFIG_DICT_1
            },
            'exclude': {
                'bigQuery': {
                    'projectName': 'exclude_project_1',
                    'sqlQuery': 'exclude_query_1'
                }
            }
        })
        assert config.include.bigquery.project_name == BIGQUERY_SOURCE_CONFIG_DICT_1['projectName']
        assert config.include.bigquery.sql_query == BIGQUERY_SOURCE_CONFIG_DICT_1['sqlQuery']
        assert config.exclude
        assert config.exclude.bigquery.project_name == 'exclude_project_1'
        assert config.exclude.bigquery.sql_query == 'exclude_query_1'


class TestBigQueryTargetConfig:
    def test_should_read_project_dataset_and_table_name(self):
        config = BigQueryTargetConfig.from_dict({
            'projectName': 'project1',
            'datasetName': 'dataset1',
            'tableName': 'table1'
        })
        assert config.project_name == 'project1'
        assert config.dataset_name == 'dataset1'
        assert config.table_name == 'table1'


class TestStateFileConfig:
    def test_should_read_bucket_and_object(self):
        config = StateFileConfig.from_dict({
            'bucketName': 'bucket1',
            'objectName': 'object1'
        })
        assert config.bucket_name == 'bucket1'
        assert config.object_name == 'object1'


class TestStrToBool:
    def test_should_return_true_for_lower_case_true(self):
        assert str_to_bool('true') is True

    def test_should_return_true_for_upper_case_true(self):
        assert str_to_bool('TRUE') is True

    def test_should_return_false_for_false(self):
        assert str_to_bool('false') is False

    def test_should_return_default_value_for_empty_value_when_it_is_defined(self):
        assert str_to_bool('', default_value=False) is False
        assert str_to_bool('', default_value=True) is True

    def test_should_raise_error_for_invalid_value(self):
        with pytest.raises(ValueError):
            str_to_bool('invalid')


class TestGetResolvedParameterValuesFromFilePathEnvName:
    def test_should_read_value_from_file(self, mock_env: dict, tmp_path: Path):
        file_path = tmp_path / 'file'
        file_path.write_text('value1')
        mock_env['env1'] = str(file_path)
        params = get_resolved_parameter_values_from_file_path_env_name([{
            'parameterName': 'param1',
            'filePathEnvName': 'env1'
        }])
        assert params == {
            'param1': 'value1'
        }


class TestGetEnvironmentVariableValue:
    def test_should_return_env_simple_optional_value(self, mock_env: dict):
        mock_env['key1'] = 'value1'
        assert get_environment_variable_value('key1', required=False) == 'value1'

    def test_should_return_env_simple_required_value(self, mock_env: dict):
        mock_env['key1'] = 'value1'
        assert get_environment_variable_value('key1', required=True) == 'value1'

    def test_should_convert_value_value(self, mock_env: dict):
        mock_env['key1'] = '12345'
        assert get_environment_variable_value('key1', int) == 12345

    def test_should_return_none_if_optional_key_does_not_exist(self, mock_env: dict):
        mock_env.update({})
        assert get_environment_variable_value('key1', required=False) is None

    def test_should_return_default_value_if_optional_key_does_not_exist(self, mock_env: dict):
        mock_env.update({})
        assert (
            get_environment_variable_value('key1', required=False, default_value='empty')
            == 'empty'
        )

    def test_should_return_raise_error_if_required_key_does_not_exist(self, mock_env: dict):
        mock_env.update({})
        with pytest.raises(KeyError):
            get_environment_variable_value('key1', required=True)

    def test_should_return_raise_error_if_required_key_is_blank(self, mock_env: dict):
        mock_env.update({
            'key1': ''
        })
        with pytest.raises(KeyError):
            get_environment_variable_value('key1', required=True)


class TestMappingConfig:
    def test_should_read_simple_dict(self):
        config = MappingConfig.from_dict({KEY_1: VALUE_1})
        assert config.mapping == {KEY_1: VALUE_1}

    def test_should_include_simple_value_in_str_repr_and_printable_mapping(self):
        config = MappingConfig.from_dict({KEY_1: VALUE_1})
        assert config.printable_mapping == {KEY_1: VALUE_1}
        assert VALUE_1 in str(config)
        assert VALUE_1 in repr(config)

    @pytest.mark.usefixtures('existing_secret_file_and_env_1')
    def test_should_read_from_env_file(self):
        config = MappingConfig.from_dict({
            'parametersFromFile': [PARAMETERS_FROM_FILE_CONFIG_DICT_1]
        })
        LOGGER.debug('config: %r', config)
        assert config.mapping == {KEY_1: VALUE_1}

    @pytest.mark.usefixtures('existing_secret_file_and_env_1')
    def test_should_not_include_env_file_values_in_str_repr_and_printable_mapping(self):
        config = MappingConfig.from_dict({
            'parametersFromFile': [PARAMETERS_FROM_FILE_CONFIG_DICT_1]
        })
        LOGGER.debug('config: %r', config)
        assert config.mapping == {KEY_1: VALUE_1}
        assert config.printable_mapping == {KEY_1: SECRET_VALUE_PLACEHOLDER}
        assert VALUE_1 not in str(config)
        assert VALUE_1 not in repr(config)


class TestParseKeyPath:
    def test_should_return_empty_list_for_none(self):
        assert parse_key_path(None) == []

    def test_should_return_passed_in_list_if_list(self):
        assert parse_key_path(['parent', 'child']) == ['parent', 'child']

    def test_should_split_string_path_by_dot(self):
        assert parse_key_path('parent.child') == ['parent', 'child']

    def test_should_raise_error_if_other_type(self):
        with pytest.raises(TypeError):
            parse_key_path({'set'})


class TestParseRequiredNonEmptyKeyPath:
    def test_should_raise_error_for_none(self):
        with pytest.raises(ValueError):
            parse_required_non_empty_key_path(None)

    def test_should_raise_error_for_empty_list(self):
        with pytest.raises(ValueError):
            parse_required_non_empty_key_path([])

    def test_should_return_passed_in_list_if_not_empty(self):
        assert parse_required_non_empty_key_path(
            ['parent', 'child']
        ) == ['parent', 'child']
