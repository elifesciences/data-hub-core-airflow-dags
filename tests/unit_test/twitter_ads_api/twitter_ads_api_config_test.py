from data_pipeline.twitter_ads_api.twitter_ads_api_config import (
    TwitterAdsApiConfig
)

RESOURCE = 'resource_1'
SECRET_FILE_ENV_VAR = 'secret_file_env_name_1'

SOURCE_CONFIG = {
    'resource': RESOURCE
}

PROJECT_NAME = 'project_1'
DATASET_NAME = 'dataset_1'
TABLE_NAME = 'table_1'

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
