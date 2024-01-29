from data_pipeline.generic_web_api.request_builder import CiviWebApiDynamicRequestBuilder
from data_pipeline.utils.pipeline_config import BigQueryIncludeExcludeSourceConfig, ConfigKeys
from data_pipeline.generic_web_api.generic_web_api_config import (
    WebApiResponseConfig,
    get_web_api_config_id,
    MultiWebApiConfig,
    WebApiConfig
)
from data_pipeline.generic_web_api.generic_web_api_config_typing import (
    WebApiResponseConfigDict
)
from tests.unit_test.generic_web_api.test_data import (
    DATASET_1,
    MINIMAL_WEB_API_CONFIG_DICT,
    TABLE_1
)


BIGQUERY_SOURCE_CONFIG_DICT_1 = {
    'projectName': 'project1',
    'sqlQuery': 'query1'
}

BIGQUERY_INCLUDE_EXCLUDE_SOURCE_CONFIG_DICT_1 = {
    'include': {'bigQuery': BIGQUERY_SOURCE_CONFIG_DICT_1}
}

RESPONSE_CONFIG_DICT_1: WebApiResponseConfigDict = {
    'itemsKeyFromResponseRoot': ['items']
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


class TestWebApiResponseConfig:
    def test_should_return_object_for_none(self):
        response_config = WebApiResponseConfig.from_dict(None)
        assert response_config is not None

    def test_should_read_from_root_paths(self):
        response_config = WebApiResponseConfig.from_dict({
            'itemsKeyFromResponseRoot': ['item-1'],
            'nextPageCursorKeyFromResponseRoot': ['next-cursor-1'],
            'totalItemsCountKeyFromResponseRoot': ['total-1']
        })
        assert response_config.items_key_path_from_response_root == ['item-1']
        assert response_config.next_page_cursor_key_path_from_response_root == ['next-cursor-1']
        assert response_config.total_item_count_key_path_from_response_root == ['total-1']

    def test_should_read_record_timestamp_path(self):
        response_config = WebApiResponseConfig.from_dict({
            'recordTimestamp': {
                'itemTimestampKeyFromItemRoot': ['timestamp-1']
            }
        })
        assert response_config.item_timestamp_key_path_from_item_root == ['timestamp-1']

    def test_should_read_fields_to_return(self):
        response_config = WebApiResponseConfig.from_dict({
            'fieldsToReturn': ['field-1', 'field-2']
        })
        assert response_config.fields_to_return == ['field-1', 'field-2']


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

    def test_should_read_response_config(self):
        web_api_config = WebApiConfig.from_dict({
            **MINIMAL_WEB_API_CONFIG_DICT,
            'response': RESPONSE_CONFIG_DICT_1
        })
        assert web_api_config.response == WebApiResponseConfig.from_dict(RESPONSE_CONFIG_DICT_1)

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
