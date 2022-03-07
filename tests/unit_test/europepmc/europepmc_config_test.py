from data_pipeline.europepmc.europepmc_config import (
    EuropePmcConfig
)


SEARCH_QUERY_1 = 'query 1'

FIELDS_TO_RETURN_1 = ['title', 'doi']

CONFIG_DICT_1 = {
    'europePmc': [{
        'source': {
            'search': {
                'query': SEARCH_QUERY_1
            },
            'fieldsToReturn': FIELDS_TO_RETURN_1
        }
    }]
}


class TestEuropePmcConfig:
    def test_should_read_search_query(self):
        config = EuropePmcConfig.from_dict(CONFIG_DICT_1)
        assert config.source.search.query == SEARCH_QUERY_1
        assert config.source.fields_to_return == FIELDS_TO_RETURN_1
