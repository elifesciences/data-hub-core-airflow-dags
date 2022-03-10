from data_pipeline.europepmc.europepmc_config import (
    EuropePmcConfig
)


API_URL_1 = 'https://api1'

SEARCH_QUERY_1 = 'query 1'

FIELDS_TO_RETURN_1 = ['title', 'doi']

PROJECT_NAME_1 = 'project1'
DATASET_NAME_1 = 'dataset1'
TABLE_NAME_1 = 'table1'

TARGET_1 = {
    'projectName': PROJECT_NAME_1,
    'datasetName': DATASET_NAME_1,
    'tableName': TABLE_NAME_1
}

SOURCE_WITHOUT_FIELDS_TO_RETURN_1 = {
    'apiUrl': API_URL_1,
    'search': {
        'query': SEARCH_QUERY_1
    }
}

SOURCE_WITH_FIELDS_TO_RETURN_1 = {
    **SOURCE_WITHOUT_FIELDS_TO_RETURN_1,
    'fieldsToReturn': FIELDS_TO_RETURN_1
}


CONFIG_DICT_WITHOUT_FIELDS_TO_RETURN_1 = {
    'europePmc': [{
        'source': SOURCE_WITHOUT_FIELDS_TO_RETURN_1,
        'target': TARGET_1
    }]
}


CONFIG_DICT_WITH_FIELDS_TO_RETURN_1 = {
    'europePmc': [{
        'source': SOURCE_WITH_FIELDS_TO_RETURN_1,
        'target': TARGET_1
    }]
}


CONFIG_DICT_1 = CONFIG_DICT_WITHOUT_FIELDS_TO_RETURN_1


class TestEuropePmcConfig:
    def test_should_read_api_url(self):
        config = EuropePmcConfig.from_dict(CONFIG_DICT_1)
        assert config.source.api_url == API_URL_1

    def test_should_read_search_query(self):
        config = EuropePmcConfig.from_dict(CONFIG_DICT_1)
        assert config.source.search.query == SEARCH_QUERY_1

    def test_should_read_fields_to_return(self):
        config = EuropePmcConfig.from_dict(CONFIG_DICT_WITH_FIELDS_TO_RETURN_1)
        assert config.source.fields_to_return == FIELDS_TO_RETURN_1

    def test_should_allow_fields_to_return_not_be_specified(self):
        config = EuropePmcConfig.from_dict(CONFIG_DICT_WITHOUT_FIELDS_TO_RETURN_1)
        assert config.source.fields_to_return is None

    def test_should_read_target_project_dataset_and_table_name(self):
        config = EuropePmcConfig.from_dict(CONFIG_DICT_1)
        assert config.target.project_name == PROJECT_NAME_1
        assert config.target.dataset_name == DATASET_NAME_1
        assert config.target.table_name == TABLE_NAME_1
