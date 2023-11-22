from data_pipeline.generic_web_api.request_builder import CiviWebApiDynamicRequestBuilder
from data_pipeline.utils.pipeline_config import BigQueryIncludeExcludeSourceConfig, ConfigKeys
from data_pipeline.generic_web_api.generic_web_api_config import (
    get_web_api_config_id,
    MultiWebApiConfig,
    WebApiConfig
)
from data_pipeline.generic_web_api.generic_web_api_config_typing import (
    WebApiConfigDict
)


DATASET_1 = 'dataset_1'
TABLE_1 = 'table_1'

BIGQUERY_SOURCE_CONFIG_DICT_1 = {
    'projectName': 'project1',
    'sqlQuery': 'query1'
}

BIGQUERY_INCLUDE_EXCLUDE_SOURCE_CONFIG_DICT_1 = {
    'include': {'bigQuery': BIGQUERY_SOURCE_CONFIG_DICT_1}
}

MINIMAL_WEB_API_CONFIG_DICT: WebApiConfigDict = {
    'dataPipelineId': 'pipeline_1',
    'gcpProjectName': 'project_1',
    'importedTimestampFieldName': 'imported_timestamp_1',
    'dataset': DATASET_1,
    'table': TABLE_1,
    'dataUrl': {
        'urlExcludingConfigurableParameters': 'url_1'
    }
}


class TestGetWebApiConfigId:
    def test_should_use_id_from_config(self):
        assert get_web_api_config_id({
            ConfigKeys.DATA_PIPELINE_CONFIG_ID: '123'
        }, index=0) == '123'

    def test_should_use_output_table_from_config_and_index(self):
        assert get_web_api_config_id({'table': 'table1'}, index=0) == 'table1_0'

    def test_should_fallback_to_index(self):
        assert get_web_api_config_id({'other': 'x'}, index=0) == '0'


class TestMultiWebApiConfig:
    def test_should_keep_existing_id_of_web_config(self):
        multi_web_api_config = MultiWebApiConfig({
            'webApi': [{
                ConfigKeys.DATA_PIPELINE_CONFIG_ID: '123'
            }]
        })
        assert multi_web_api_config.web_api_config[0][
            ConfigKeys.DATA_PIPELINE_CONFIG_ID
        ] == '123'

    def test_should_add_id_to_web_config(self):
        multi_web_api_config = MultiWebApiConfig({
            'webApi': [{'table': 'table1'}]
        })
        assert multi_web_api_config.web_api_config[0][
            ConfigKeys.DATA_PIPELINE_CONFIG_ID
        ] == 'table1_0'


class TestWebApiConfig:
    def test_should_parse_dataset_and_table(self):
        web_api_config = WebApiConfig.from_dict(MINIMAL_WEB_API_CONFIG_DICT)
        assert web_api_config.dataset_name == DATASET_1
        assert web_api_config.table_name == TABLE_1

    def test_should_set_headers_field_to_empty_dict_if_headers_not_defined(self):
        web_api_config = WebApiConfig.from_dict(MINIMAL_WEB_API_CONFIG_DICT)
        assert web_api_config.headers.mapping == {}

    def test_should_read_api_headers(self):
        headers = {'key1': 'value1'}
        web_api_config = WebApiConfig.from_dict({
            **MINIMAL_WEB_API_CONFIG_DICT,
            'headers': headers
        })
        assert web_api_config.headers.mapping == headers

    def test_should_read_bigquery_source_config(self):
        web_api_config = WebApiConfig.from_dict({
            **MINIMAL_WEB_API_CONFIG_DICT,
            'source': {
                'include': {
                    'bigQuery': BIGQUERY_SOURCE_CONFIG_DICT_1
                }
            }
        })
        assert web_api_config.source
        assert (
            web_api_config.source
            == BigQueryIncludeExcludeSourceConfig.from_dict(
                BIGQUERY_INCLUDE_EXCLUDE_SOURCE_CONFIG_DICT_1
            )
        )

    def test_should_set_batch_size_to_none_by_default(self):
        web_api_config = WebApiConfig.from_dict(MINIMAL_WEB_API_CONFIG_DICT)
        assert web_api_config.batch_size is None

    def test_should_read_batch_size(self):
        web_api_config = WebApiConfig.from_dict({
            **MINIMAL_WEB_API_CONFIG_DICT,
            'batchSize': 123
        })
        assert web_api_config.batch_size == 123

    def test_should_read_request_builder_name_and_parameters(self):
        web_api_config = WebApiConfig.from_dict({
            **MINIMAL_WEB_API_CONFIG_DICT,
            'requestBuilder': {
                'name': 'civi',
                'parameters': {
                    'fieldsToReturn': ['field1', 'field2']
                }
            }
        })
        assert isinstance(
            web_api_config.dynamic_request_builder,
            CiviWebApiDynamicRequestBuilder
        )
        assert web_api_config.dynamic_request_builder.request_builder_parameters == {
            'fieldsToReturn': ['field1', 'field2']
        }

    def test_should_read_deprecated_request_builder_name_and_parameters(self):
        web_api_config = WebApiConfig.from_dict({
            **MINIMAL_WEB_API_CONFIG_DICT,
            'urlSourceType': {
                'name': 'civi',
                'sourceTypeSpecificValues': {
                    'fieldsToReturn': ['field1', 'field2']
                }
            }
        })
        assert isinstance(
            web_api_config.dynamic_request_builder,
            CiviWebApiDynamicRequestBuilder
        )
        assert web_api_config.dynamic_request_builder.request_builder_parameters == {
            'fieldsToReturn': ['field1', 'field2']
        }
