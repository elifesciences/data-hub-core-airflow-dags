from data_pipeline.related_articles.related_articles_config import (
    RelatedArticlesConfig
)

GIT_REPO_URL = 'git_repo_url'
DIRECTORY_NAME = 'directory_name'

SOURCE_CONFIG = {
    'gitRepoUrl': GIT_REPO_URL,
    'directoryName': DIRECTORY_NAME
}

PROJECT_NAME = 'project'
DATASET_NAME = 'dataset'
TABLE_NAME = 'table'

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
    return {'relatedArticles': [item_dict]}

CONFIG_DICT = get_config_for_item_config_dict(ITEM_CONFIG_DICT)

class TestRelatedArticlesConfig:
    def test_should_read_git_repo_url(self):
        config = RelatedArticlesConfig.from_dict(CONFIG_DICT)
        assert config.source.git_repo_url == GIT_REPO_URL

    def test_should_read_directory_name(self):
        config = RelatedArticlesConfig.from_dict(CONFIG_DICT)
        assert config.source.directory_name == DIRECTORY_NAME
