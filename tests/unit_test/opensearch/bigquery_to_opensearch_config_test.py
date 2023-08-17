from data_pipeline.opensearch.bigquery_to_opensearch_config import (
    BigQueryToOpenSearchConfig,
    OpenSearchTargetConfig
)
from data_pipeline.utils.pipeline_config import BigQuerySourceConfig


BIGQUERY_SOURCE_CONFIG_DICT_1 = {
    'projectName': 'project1',
    'sqlQuery': 'query1'
}


OPENSEARCH_TARGET_CONFIG_DICT_1 = {
    'hostname': 'hostname1',
    'port': 9200
}


CONFIG_DICT_1: dict = {
    'source': {'bigQuery': BIGQUERY_SOURCE_CONFIG_DICT_1},
    'target': {'opensearch': OPENSEARCH_TARGET_CONFIG_DICT_1}
}


class TestOpenSearchTargetConfig:
    def test_should_read_hostname_and_port(self):
        opensearch_target_config = OpenSearchTargetConfig.from_dict(
            OPENSEARCH_TARGET_CONFIG_DICT_1
        )
        assert opensearch_target_config.hostname == OPENSEARCH_TARGET_CONFIG_DICT_1['hostname']
        assert opensearch_target_config.port == OPENSEARCH_TARGET_CONFIG_DICT_1['port']


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
