from data_pipeline.twitter_ads_api.twitter_ads_api_config import (
    TwitterAdsApiConfig
)

RESOURCE = 'resource_1'
SECRETS = {'key1': 'value1', 'key2': 'value2'}

SQL_QUERY = 'query 1'

PROJECT_NAME = 'project_1'
DATASET_NAME = 'dataset_1'
TABLE_NAME = 'table_1'

BIGQUERY_SOURCE_CONFIG_DICT_1 = {
    'projectName': PROJECT_NAME,
    'sqlQuery': SQL_QUERY
}

PARAM_NAMES = ['param_name_1', 'param_name_2']

SOURCE_CONFIG = {
    'resource': RESOURCE,
    'secrets': SECRETS
}

TARGET_CONFIG = {
    'projectName': PROJECT_NAME,
    'datasetName': DATASET_NAME,
    'tableName': TABLE_NAME
}

ITEM_CONFIG_DICT = {
    'source': SOURCE_CONFIG,
    'target': TARGET_CONFIG
}


def get_config_for_item_config_dict(item_dict: dict) -> dict:
    return {'twitterAdsApi': [item_dict]}


CONFIG_DICT = get_config_for_item_config_dict(ITEM_CONFIG_DICT)


class TestTwitterAdsApiConfig:
    def test_should_read_resource(self):
        config = TwitterAdsApiConfig.from_dict(CONFIG_DICT)
        assert config.source.resource == RESOURCE

    def test_should_read_target_project_dataset_and_table_name(self):
        config = TwitterAdsApiConfig.from_dict(CONFIG_DICT)
        assert config.target.project_name == PROJECT_NAME
        assert config.target.dataset_name == DATASET_NAME
        assert config.target.table_name == TABLE_NAME

    def test_should_read_api_secrets(self):
        secrets = {'key1': 'value1', 'key2': 'value2'}
        config = TwitterAdsApiConfig.from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT,
            'source': {
                **SOURCE_CONFIG,
                'secrets': secrets
            }
        }))
        assert config.source.secrets.mapping == secrets

    def test_should_read_api_secrets_by_key(self):
        secrets = {'key1': 'value1', 'key2': 'value2'}
        config = TwitterAdsApiConfig.from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT,
            'source': {
                **SOURCE_CONFIG,
                'secrets': secrets
            }
        }))
        assert config.source.secrets.mapping['key1'] == secrets['key1']

    def test_should_read_param_names_if_defined(self):
        config = TwitterAdsApiConfig.from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT,
            'source': {
                **SOURCE_CONFIG,
                'paramNames': PARAM_NAMES
            }
        }))
        assert config.source.param_names == PARAM_NAMES

    def test_should_read_param_names_as_an_empty_list_if_not_defined(self):
        config = TwitterAdsApiConfig.from_dict(CONFIG_DICT)
        assert config.source.param_names == []

    def test_should_read_source_bigquery_sql_query_if_defined(self):
        config = TwitterAdsApiConfig.from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT,
            'source': {
                **SOURCE_CONFIG,
                'bigQuery': BIGQUERY_SOURCE_CONFIG_DICT_1
            }
        }))
        assert config.source.bigquery.sql_query == SQL_QUERY

    def test_should_read_source_bigquery_project_name_if_defined(self):
        config = TwitterAdsApiConfig.from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT,
            'source': {
                **SOURCE_CONFIG,
                'bigQuery': BIGQUERY_SOURCE_CONFIG_DICT_1
            }
        }))
        assert config.source.bigquery.project_name == PROJECT_NAME

    def test_should_return_empty_str_for_bq_project_name_if_not_defined(self):
        config = TwitterAdsApiConfig.from_dict(CONFIG_DICT)
        print(config.source.bigquery)
        assert config.source.bigquery.project_name == ''

    def test_should_return_empty_str_for_bq_sql_query_if_not_defined(self):
        config = TwitterAdsApiConfig.from_dict(CONFIG_DICT)
        print(config.source.bigquery)
        assert config.source.bigquery.sql_query == ''
