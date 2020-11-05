from data_pipeline.utils.pipeline_config import ConfigKeys
from data_pipeline.generic_web_api.generic_web_api_config import (
    get_web_api_config_id,
    MultiWebApiConfig
)


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
