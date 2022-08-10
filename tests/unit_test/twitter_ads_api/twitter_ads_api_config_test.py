from data_pipeline.twitter_ads_api.twitter_ads_api_config import (
    TwitterAdsApiConfig
)

RESOURCE_1 = 'resource_1'
RESOURCE_2 = 'resource_2'
SECRETS = {'key1': 'value1', 'key2': 'value2'}

SOURCE_CONFIG_1 = {
    'resource': RESOURCE_1,
    'secrets': SECRETS
}

SOURCE_CONFIG_2 = {
    'resource': RESOURCE_2,
    'secrets': SECRETS
}

SQL_QUERY = 'query 1'

PROJECT_NAME = 'project_1'
DATASET_NAME = 'dataset_1'
TABLE_NAME = 'table_1'

PARAM_VALUE_FROM_BQ_DICT = {
    'projectName': PROJECT_NAME,
    'sqlQuery': SQL_QUERY
}

PARAM_NAME_FOR_BQ_VALUE = 'param_name_for_bq_value_1'
PARAM_NAME_FOR_START_TIME = 'param_name_for_start_time_1'
PARAM_NAME_FOR_END_TIME = 'param_name_for_end_time_1'

TARGET_CONFIG = {
    'projectName': PROJECT_NAME,
    'datasetName': DATASET_NAME,
    'tableName': TABLE_NAME
}

ITEM_CONFIG_DICT_1 = {
    'source': SOURCE_CONFIG_1,
    'target': TARGET_CONFIG
}

ITEM_CONFIG_DICT_2 = {
    'source': SOURCE_CONFIG_2,
    'target': TARGET_CONFIG
}


def get_config_for_item_config_dict(item_dict_1: dict, item_dict_2: dict) -> dict:
    return {'twitterAdsApi': [item_dict_1, item_dict_2]}


CONFIG_DICT = get_config_for_item_config_dict(ITEM_CONFIG_DICT_1, ITEM_CONFIG_DICT_2)


class TestTwitterAdsApiConfig:
    def test_should_read_resource(self):
        config = TwitterAdsApiConfig.parse_config_list_from_dict(CONFIG_DICT)
        assert config[0].source.resource == RESOURCE_1
        assert config[1].source.resource == RESOURCE_2

    def test_should_read_target_project_dataset_and_table_name(self):
        config = TwitterAdsApiConfig.parse_config_list_from_dict(CONFIG_DICT)
        assert config[0].target.project_name == PROJECT_NAME
        assert config[0].target.dataset_name == DATASET_NAME
        assert config[0].target.table_name == TABLE_NAME

    def test_should_read_api_secrets(self):
        secrets = {'key1': 'value1', 'key2': 'value2'}
        config = TwitterAdsApiConfig.parse_config_list_from_dict(
            get_config_for_item_config_dict(
                {
                    **ITEM_CONFIG_DICT_1,
                    'source': {
                        **SOURCE_CONFIG_1,
                        'secrets': secrets
                    }
                },
                {
                    **ITEM_CONFIG_DICT_2,
                    'source': {
                        **SOURCE_CONFIG_2,
                        'secrets': secrets
                    }
                }
            )
        )
        assert config[0].source.secrets.mapping == secrets
        assert config[1].source.secrets.mapping == secrets

    def test_should_read_api_secrets_by_key(self):
        secrets = {'key1': 'value1', 'key2': 'value2'}
        config = TwitterAdsApiConfig.parse_config_list_from_dict(
            get_config_for_item_config_dict(
                {
                    **ITEM_CONFIG_DICT_1,
                    'source': {
                        **SOURCE_CONFIG_1,
                        'secrets': secrets
                    }
                },
                {
                    **ITEM_CONFIG_DICT_2,
                    'source': {
                        **SOURCE_CONFIG_2,
                        'secrets': secrets
                    }
                }
            )
        )
        assert config[0].source.secrets.mapping['key1'] == secrets['key1']
        assert config[1].source.secrets.mapping['key1'] == secrets['key1']

    def test_should_read_param_name_for_bq_value_if_defined_and_should_return_an_empty_str_if_not(
        self
    ):
        config = TwitterAdsApiConfig.parse_config_list_from_dict(
            get_config_for_item_config_dict(
                {
                    **ITEM_CONFIG_DICT_1,
                    'source': {
                        **SOURCE_CONFIG_1,
                        'paramNameForBqValue': PARAM_NAME_FOR_BQ_VALUE
                    }
                },
                ITEM_CONFIG_DICT_2
            )
        )
        assert config[0].source.param_name_for_bq_value == PARAM_NAME_FOR_BQ_VALUE
        assert config[1].source.param_name_for_bq_value == ''

    def test_should_read_param_name_for_start_time_if_defined_and_should_return_an_empty_str_if_not(
        self
    ):
        config = TwitterAdsApiConfig.parse_config_list_from_dict(
            get_config_for_item_config_dict(
                {
                    **ITEM_CONFIG_DICT_1,
                    'source': {
                        **SOURCE_CONFIG_1,
                        'paramNameForStartTime': PARAM_NAME_FOR_START_TIME
                    }
                },
                ITEM_CONFIG_DICT_2
            )
        )
        assert config[0].source.param_name_for_start_time == PARAM_NAME_FOR_START_TIME
        assert config[1].source.param_name_for_start_time == ''

    def test_should_read_param_name_for_end_time_if_defined_and_should_return_an_empty_str_if_not(
        self
    ):
        config = TwitterAdsApiConfig.parse_config_list_from_dict(
            get_config_for_item_config_dict(
                {
                    **ITEM_CONFIG_DICT_1,
                    'source': {
                        **SOURCE_CONFIG_1,
                        'paramNameForEndTime': PARAM_NAME_FOR_END_TIME
                    }
                },
                ITEM_CONFIG_DICT_2
            )
        )
        assert config[0].source.param_name_for_end_time == PARAM_NAME_FOR_END_TIME
        assert config[1].source.param_name_for_end_time == ''

    def test_should_read_params_from_bq_sql_query_if_defined_otherwise_should_return_an_empty_str(
        self
    ):
        config = TwitterAdsApiConfig.parse_config_list_from_dict(
            get_config_for_item_config_dict(
                {
                    **ITEM_CONFIG_DICT_1,
                    'source': {
                        **SOURCE_CONFIG_1,
                        'paramValueFromBigQuery': PARAM_VALUE_FROM_BQ_DICT
                    }
                },
                ITEM_CONFIG_DICT_2
            )
        )
        assert config[0].source.param_value_from_bigquery.sql_query == SQL_QUERY
        assert config[0].source.param_value_from_bigquery.project_name == PROJECT_NAME
        assert config[1].source.param_value_from_bigquery.sql_query == ''
        assert config[1].source.param_value_from_bigquery.project_name == ''
        assert config[1].source.param_value_from_bigquery.sql_query == ''
