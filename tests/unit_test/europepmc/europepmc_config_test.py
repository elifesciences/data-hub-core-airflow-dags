from data_pipeline.europepmc.europepmc_config import (
    EuropePmcConfig
)


API_URL_1 = 'https://api1'

SEARCH_QUERY_1 = 'query 1'

FIELDS_TO_RETURN_1 = ['title', 'doi']

TABLE_NAME_1 = 'table1'

CONFIG_DICT_1 = {
    'europePmc': [{
        'source': {
            'apiUrl': API_URL_1,
            'search': {
                'query': SEARCH_QUERY_1
            },
            'fieldsToReturn': FIELDS_TO_RETURN_1
        },
        'target': {
            'tableName': TABLE_NAME_1
        }
    }]
}


class TestEuropePmcConfig:
    def test_should_read_api_url(self):
        config = EuropePmcConfig.from_dict(CONFIG_DICT_1)
        assert config.source.api_url == API_URL_1


    def test_should_read_search_query(self):
        config = EuropePmcConfig.from_dict(CONFIG_DICT_1)
        assert config.source.search.query == SEARCH_QUERY_1

    def test_should_read_fields_to_return(self):
        config = EuropePmcConfig.from_dict(CONFIG_DICT_1)
        assert config.source.fields_to_return == FIELDS_TO_RETURN_1

    def test_should_read_target_table_name(self):
        config = EuropePmcConfig.from_dict(CONFIG_DICT_1)
        assert config.target.table_name == TABLE_NAME_1
