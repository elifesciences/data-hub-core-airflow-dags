from data_pipeline.semantic_scholar.semantic_scholar_recommendation_config import (
    DEFAULT_BATCH_SIZE,
    SemanticScholarRecommendationConfig
)


API_URL_1 = 'https://api1'


PROJECT_NAME_1 = 'project1'
DATASET_NAME_1 = 'dataset1'
TABLE_NAME_1 = 'table1'


MATRIX_1 = {
    'list': {
        'include': {
            'bigQuery': {
                'projectName': PROJECT_NAME_1,
                'sqlQuery': 'query1'
            }
        }
    }
}


SOURCE_1 = {
    'apiUrl': API_URL_1
}


TARGET_1 = {
    'projectName': PROJECT_NAME_1,
    'datasetName': DATASET_NAME_1,
    'tableName': TABLE_NAME_1
}


ITEM_CONFIG_DICT_1 = {
    'matrix': MATRIX_1,
    'source': SOURCE_1,
    'target': TARGET_1
}


def get_config_for_item_config_dict(item_dict: dict) -> dict:
    return {'semanticScholarRecommendation': [item_dict]}


CONFIG_DICT_1 = get_config_for_item_config_dict(ITEM_CONFIG_DICT_1)


class TestSemanticScholarRecommendationConfig:
    def test_should_read_api_url(self):
        config = SemanticScholarRecommendationConfig.from_dict(CONFIG_DICT_1)
        assert config.source.api_url == API_URL_1

    def test_should_read_api_params(self):
        params = {'param1': 'value1'}
        config = SemanticScholarRecommendationConfig.from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT_1,
            'source': {
                **SOURCE_1,
                'params': params
            }
        }))
        assert config.source.params == params

    def test_should_read_batch_size(self):
        config = SemanticScholarRecommendationConfig.from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT_1,
            'batchSize': 123
        }))
        assert config.batch_size == 123

    def test_should_use_default_batch_size(self):
        assert 'batchSize' not in ITEM_CONFIG_DICT_1
        config = SemanticScholarRecommendationConfig.from_dict(get_config_for_item_config_dict(
            ITEM_CONFIG_DICT_1
        ))
        assert config.batch_size == DEFAULT_BATCH_SIZE

    def test_should_read_target_project_dataset_and_table_name(self):
        config = SemanticScholarRecommendationConfig.from_dict(CONFIG_DICT_1)
        assert config.target.project_name == PROJECT_NAME_1
        assert config.target.dataset_name == DATASET_NAME_1
        assert config.target.table_name == TABLE_NAME_1

    def test_should_read_matrix_variables_without_exclude(self):
        config = SemanticScholarRecommendationConfig.from_dict(CONFIG_DICT_1)
        assert config.matrix.variables.keys() == {'list'}
        variable_config = list(config.matrix.variables.values())[0]
        assert variable_config.include.bigquery.project_name == PROJECT_NAME_1
        assert variable_config.include.bigquery.sql_query == (
            MATRIX_1['list']['include']['bigQuery']['sqlQuery']
        )
        assert not variable_config.exclude

    def test_should_read_matrix_variables_with_exclude(self):
        matrix_config_dict = {
            'list': {
                'include': MATRIX_1['list']['include'],
                'exclude': {
                    'bigQuery': {
                        'projectName': PROJECT_NAME_1,
                        'sqlQuery': 'query2'
                    },
                    'keyFieldNameFromInclude': 'list_key'
                }
            }
        }
        config = SemanticScholarRecommendationConfig.from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT_1,
            'matrix': matrix_config_dict
        }))
        assert config.matrix.variables.keys() == {'list'}
        variable_config = list(config.matrix.variables.values())[0]
        assert variable_config.include.bigquery.project_name == PROJECT_NAME_1
        assert variable_config.include.bigquery.sql_query == (
            matrix_config_dict['list']['include']['bigQuery']['sqlQuery']
        )
        assert variable_config.exclude.bigquery.project_name == PROJECT_NAME_1
        assert variable_config.exclude.bigquery.sql_query == (
            matrix_config_dict['list']['exclude']['bigQuery']['sqlQuery']
        )
