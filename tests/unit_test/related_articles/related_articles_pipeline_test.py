from unittest.mock import MagicMock, patch
import pytest

from data_pipeline.related_articles import(
    related_articles_pipeline as related_articles_pipeline_module
)
from data_pipeline.related_articles.related_articles_config import RelatedArticlesSourceConfig

from data_pipeline.related_articles.related_articles_pipeline import(
    get_url_of_xml_file_directory_from_repo,
    iter_decoded_xml_file_content,
    fetch_related_article_from_elife_article_xml_repo_and_load_into_bigquery
)


@pytest.fixture(name='get_json_response_from_url_mock')
def _get_json_response_from_url_mock():
    with patch.object(related_articles_pipeline_module, 'get_json_response_from_url') as mock:
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

GITHUB_TREE_REPONSE_1={
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
