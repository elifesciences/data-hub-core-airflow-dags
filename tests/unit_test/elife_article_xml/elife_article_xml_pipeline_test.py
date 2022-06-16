from unittest.mock import MagicMock, patch
import pytest

from data_pipeline.elife_article_xml import(
    elife_article_xml_pipeline as elife_article_xml_pipeline_module
)
from data_pipeline.elife_article_xml.elife_article_xml_config import RelatedArticlesSourceConfig

from data_pipeline.elife_article_xml.elife_article_xml_pipeline import(
    get_url_of_xml_file_directory_from_repo,
    iter_xml_file_url_from_git_directory,
    fetch_related_article_from_elife_article_xml_repo_and_load_into_bigquery
)


@pytest.fixture(name='get_json_response_from_url_mock')
def _get_json_response_from_url_mock():
    with patch.object(elife_article_xml_pipeline_module, 'get_json_response_from_url') as mock:
        yield mock

@pytest.fixture(name='get_url_of_xml_file_directory_from_repo_mock')
def get_url_of_xml_file_directory_from_repo_mock():
    with patch.object(elife_article_xml_pipeline_module, 'get_url_of_xml_file_directory_from_repo') as mock:
        yield mock


# CONTENT_ENCODED = 'TWVyaGFiYSBEdW55YSEgSGVsbG8gV29ybGQh'
# CONTENT_DECODED = 'Merhaba Dunya! Hello World!'

# XML_FILE_JSON = {
#     'content': CONTENT_ENCODED
# }

MATCHING_DIRECTORY_NAME = 'matching_directory'
OTHER_DIRECTORY_NAME = 'other_directory'

MATCHING_URL = 'matching_url'
OTHER_URL = 'other_url'

GITHUB_TREE_REPONSE_1 = {
  'tree': [
    {
      'path': MATCHING_DIRECTORY_NAME,
      'url': MATCHING_URL
    },
    {
      'path': OTHER_DIRECTORY_NAME,
      'url': OTHER_URL
    }
  ]
}

EMPTY_ARTICLE_URL = 'empty_article_url'
NOT_EMPTY_ARTICLE_URL_1 = 'not_empty_article_url_1'
NOT_EMPTY_ARTICLE_URL_2 = 'not_empty_article_url_2'

GITHUB_TREE_REPONSE_2 = {
  'tree': [
    {
      'size': 0,
      'url': EMPTY_ARTICLE_URL
    },
    {
      'size': 10,
      'url': NOT_EMPTY_ARTICLE_URL_1
    },
    {
      'size': 15,
      'url': NOT_EMPTY_ARTICLE_URL_2
    }
  ]
}

GIT_REPO_URL = 'git_repo_url_1'

SOURCE_CONFIG_1 = RelatedArticlesSourceConfig(
    git_repo_url=GIT_REPO_URL,
    directory_name=MATCHING_DIRECTORY_NAME
)

class TestGetUrlOfXmlFileDirectoryFromRepo:
    def test_should_return_url_for_matching_path(
        self,
        get_json_response_from_url_mock: MagicMock
    ):
        get_json_response_from_url_mock.return_value = GITHUB_TREE_REPONSE_1
        actual_return_value = get_url_of_xml_file_directory_from_repo(SOURCE_CONFIG_1)
        assert actual_return_value == MATCHING_URL


class TestIterXmlFileUrlFromGitDirectory:
    def test_should_return_not_empty_file_url_list(
        self,
        get_json_response_from_url_mock: MagicMock,
        get_url_of_xml_file_directory_from_repo_mock: MagicMock
    ):
        get_json_response_from_url_mock.return_value = GITHUB_TREE_REPONSE_2
        get_url_of_xml_file_directory_from_repo_mock.return_value = GITHUB_TREE_REPONSE_2
        actual_return_value = list(iter_xml_file_url_from_git_directory(SOURCE_CONFIG_1))
        assert actual_return_value == [NOT_EMPTY_ARTICLE_URL_1, NOT_EMPTY_ARTICLE_URL_2]

