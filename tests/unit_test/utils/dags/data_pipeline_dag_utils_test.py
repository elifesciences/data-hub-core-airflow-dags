from data_pipeline.utils.pipeline_config import ConfigKeys
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    get_suffix_for_config
)


class TestGetSuffixForConfig:
    def test_should_return_underscore_with_id(self):
        assert get_suffix_for_config({
            ConfigKeys.DATA_PIPELINE_CONFIG_ID: '123'
        }) == '_123'

    def test_should_fallback_to_empty_string(self):
        assert get_suffix_for_config({}) == ''
