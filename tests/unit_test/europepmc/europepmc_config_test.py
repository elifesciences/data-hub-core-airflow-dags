from data_pipeline.europepmc.europepmc_config import (
    EuropePmcConfig
)


SEARCH_QUERY_1 = 'query 1'

CONFIG_DICT_1 = {
    'europePmc': [{
        'source': {
            'search': {
                'query': SEARCH_QUERY_1
            }
        }
    }]
}


class TestEuropePmcConfig:
    def test_should_read_search_query(self):
        config = EuropePmcConfig.from_dict(CONFIG_DICT_1)
        assert config.source.search.query == SEARCH_QUERY_1
