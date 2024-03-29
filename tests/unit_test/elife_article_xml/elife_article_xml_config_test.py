from data_pipeline.elife_article_xml.elife_article_xml_config import (
    ElifeArticleXmlConfig
)

GIT_REPO_URL = 'git_repo_url_1'
DIRECTORY_NAME = 'directory_name_1'
SEARCHED_XML_ELEMENTS = ['selected_xml_elements_1', 'selected_xml_elements_2']

SOURCE_CONFIG = {
    'gitRepoUrl': GIT_REPO_URL,
    'directoryName': DIRECTORY_NAME,
    'selectedXmlElements': SEARCHED_XML_ELEMENTS
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
    return {'elifeArticleXml': [item_dict]}


CONFIG_DICT = get_config_for_item_config_dict(ITEM_CONFIG_DICT)


class TestElifeArticleXmlConfig:
    def test_should_read_git_repo_url(self):
        config = ElifeArticleXmlConfig.parse_config_list_from_dict(CONFIG_DICT)
        assert config[0].source.git_repo_url == GIT_REPO_URL

    def test_should_read_directory_name(self):
        config = ElifeArticleXmlConfig.parse_config_list_from_dict(CONFIG_DICT)
        assert config[0].source.directory_name == DIRECTORY_NAME

    def test_should_read_selected_xml_elements(self):
        config = ElifeArticleXmlConfig.parse_config_list_from_dict(CONFIG_DICT)
        assert config[0].source.selected_xml_elements == SEARCHED_XML_ELEMENTS

    def test_should_read_target_project_dataset_and_table_name(self):
        config = ElifeArticleXmlConfig.parse_config_list_from_dict(CONFIG_DICT)
        assert config[0].target.project_name == PROJECT_NAME
        assert config[0].target.dataset_name == DATASET_NAME
        assert config[0].target.table_name == TABLE_NAME

    def test_should_read_api_headers(self):
        headers = {'key1': 'value1'}
        config = ElifeArticleXmlConfig.parse_config_list_from_dict(get_config_for_item_config_dict({
            **ITEM_CONFIG_DICT,
            'source': {
                **SOURCE_CONFIG,
                'headers': headers
            }
        }))
        assert config[0].source.headers.mapping == headers
