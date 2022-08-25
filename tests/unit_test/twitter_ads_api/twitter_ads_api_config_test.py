from typing import Sequence
from data_pipeline.twitter_ads_api.twitter_ads_api_config import (
    TwitterAdsApiConfig
)

RESOURCE_1 = 'resource_1'
RESOURCE_2 = 'resource_2'
SECRETS = {'key1': 'value1', 'key2': 'value2'}

SQL_QUERY = 'query_1'

PROJECT_NAME = 'project_1'
DATASET_NAME = 'dataset_1'
TABLE_NAME = 'table_1'

FROM_BIGQUERY_DICT = {
    'projectName': PROJECT_NAME,
    'sqlQuery': SQL_QUERY
}

END_PERIOD_PER_DAYS = 53

PARAMETER_VALUES_DICT = {
    'fromBigQuery': FROM_BIGQUERY_DICT,
    'maxPeriodInDays': END_PERIOD_PER_DAYS
}

NAME_FOR_ENTITY_ID_1 = 'name_for_entity_id_1'
NAME_FOR_START_DATE_1 = 'name_for_start_date_1'
NAME_FOR_END_DATE_1 = 'name_for_end_date_1'

PARAMETER_NAMES_FOR_DICT = {
    'entityId': NAME_FOR_ENTITY_ID_1,
    'startDate': NAME_FOR_START_DATE_1,
    'endDate': NAME_FOR_END_DATE_1
}

PLACEMENT_1 = 'placement_1'

API_QUERY_PARAMETERS_DICT = {
    'parameterValues': PARAMETER_VALUES_DICT,
    'parameterNamesFor': PARAMETER_NAMES_FOR_DICT
}

SOURCE_CONFIG_WITHOUT_API_QUERY_PARAMETERS = {
    'resource': RESOURCE_1,
    'secrets': SECRETS
}

SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS = {
    'resource': RESOURCE_2,
    'secrets': SECRETS,
    'apiQueryParameters': API_QUERY_PARAMETERS_DICT
}

TARGET_CONFIG = {
    'projectName': PROJECT_NAME,
    'datasetName': DATASET_NAME,
    'tableName': TABLE_NAME
}

ITEM_CONFIG_DICT_WITHOUT_API_QUERY_PARAMETERS = {
    'source': SOURCE_CONFIG_WITHOUT_API_QUERY_PARAMETERS,
    'target': TARGET_CONFIG
}

ITEM_CONFIG_DICT_WITH_API_QUERY_PARAMETERS = {
    'source': SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS,
    'target': TARGET_CONFIG
}


def get_config_for_item_config_dict(item_dict_list: Sequence[dict]) -> dict:
    return {'twitterAdsApi': item_dict_list}


CONFIG_DICT = get_config_for_item_config_dict([
    ITEM_CONFIG_DICT_WITHOUT_API_QUERY_PARAMETERS,
    ITEM_CONFIG_DICT_WITH_API_QUERY_PARAMETERS
])


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
            get_config_for_item_config_dict([
                {
                    **ITEM_CONFIG_DICT_WITHOUT_API_QUERY_PARAMETERS,
                    'source': {
                        **SOURCE_CONFIG_WITHOUT_API_QUERY_PARAMETERS,
                        'secrets': secrets
                    }
                },
                {
                    **ITEM_CONFIG_DICT_WITH_API_QUERY_PARAMETERS,
                    'source': {
                        **SOURCE_CONFIG_WITH_API_QUERY_PARAMETERS,
                        'secrets': secrets
                    }
                }
            ])
        )
        assert config[0].source.secrets.mapping == secrets
        assert config[1].source.secrets.mapping == secrets

    def test_should_read_empty_dict_for_api_query_parameters_if_not_defined(self):
        config = TwitterAdsApiConfig.parse_config_list_from_dict(
            get_config_for_item_config_dict([
                ITEM_CONFIG_DICT_WITHOUT_API_QUERY_PARAMETERS
            ])
        )
        assert config[0].source.api_query_parameters == {}

    def test_should_read_defined_parameter_values_and_empty_list_for_not_defined(
        self
    ):
        config = TwitterAdsApiConfig.parse_config_list_from_dict(
            get_config_for_item_config_dict([
                ITEM_CONFIG_DICT_WITH_API_QUERY_PARAMETERS
            ])
        )
        assert config[0].source.api_query_parameters.parameter_values.max_period_in_days == (
            END_PERIOD_PER_DAYS
        )
        assert config[0].source.api_query_parameters.parameter_values.placement_value == []

    def test_should_read_from_bigquery_sql_query_and_project_name_if_defined(self):
        config = TwitterAdsApiConfig.parse_config_list_from_dict(
            get_config_for_item_config_dict([
                ITEM_CONFIG_DICT_WITH_API_QUERY_PARAMETERS
            ])
        )
        assert config[0].source.api_query_parameters.parameter_values.from_bigquery.sql_query == (
            SQL_QUERY
        )
        assert (
            config[0].source.api_query_parameters.parameter_values.from_bigquery.project_name == (
                PROJECT_NAME
            )
        )

    def test_should_read_parameter_names_for_values_if_defined_and_none_if_not_defined(self):
        config = TwitterAdsApiConfig.parse_config_list_from_dict(
            get_config_for_item_config_dict([
                ITEM_CONFIG_DICT_WITH_API_QUERY_PARAMETERS
            ])
        )
        assert config[0].source.api_query_parameters.parameter_names_for.entity_id == (
            NAME_FOR_ENTITY_ID_1
        )
        assert config[0].source.api_query_parameters.parameter_names_for.start_date == (
            NAME_FOR_START_DATE_1
        )
        assert config[0].source.api_query_parameters.parameter_names_for.end_date == (
            NAME_FOR_END_DATE_1
        )
        assert config[0].source.api_query_parameters.parameter_names_for.placement is None
