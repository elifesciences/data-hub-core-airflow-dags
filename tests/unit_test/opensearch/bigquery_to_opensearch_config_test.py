from data_pipeline.opensearch.bigquery_to_opensearch_config import (
    BigQueryToOpenSearchConfig
)


class TestBigQueryToOpenSearchConfig:
    def test_should_load_empty_list_with_empty_config(self):
        config_list = list(BigQueryToOpenSearchConfig.parse_config_list_from_dict({
            'bigQueryToOpenSearch': []
        }))
        assert not config_list
