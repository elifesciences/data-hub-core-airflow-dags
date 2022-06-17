from unittest.mock import MagicMock, patch
import pytest

from data_pipeline.elife_article_xml import (
    elife_article_xml_pipeline as elife_article_xml_pipeline_module
)
from data_pipeline.elife_article_xml.elife_article_xml_config import ElifeArticleXmlSourceConfig

from data_pipeline.elife_article_xml.elife_article_xml_pipeline import (
    get_json_response_from_url,
    get_url_of_xml_file_directory_from_repo,
    iter_xml_file_url_from_git_directory,
    iter_decoded_xml_file_content
)


@pytest.fixture(name='requests_mock', autouse=True)
def _requests_mock():
    with patch.object(elife_article_xml_pipeline_module, 'requests') as mock:
        yield mock


@pytest.fixture(name='get_json_response_from_url_mock')
def _get_json_response_from_url_mock():
    with patch.object(elife_article_xml_pipeline_module, 'get_json_response_from_url') as mock:
        yield mock


@pytest.fixture(name='get_url_of_xml_file_directory_from_repo_mock')
def _get_url_of_xml_file_directory_from_repo_mock():
    with patch.object(
      elife_article_xml_pipeline_module, 'get_url_of_xml_file_directory_from_repo'
    ) as mock:
        yield mock


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
PROCESSED_NOT_EMPTY_ARTICLE_URL = 'existing_not_empty_article_url'

PROCESSED_FILE_URL_LIST = [PROCESSED_NOT_EMPTY_ARTICLE_URL]

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
        },
        {
            'size': 15,
            'url': PROCESSED_NOT_EMPTY_ARTICLE_URL
        }
    ]
}

GIT_REPO_URL = 'git_repo_url_1'

SOURCE_CONFIG_1 = ElifeArticleXmlSourceConfig(
    git_repo_url=GIT_REPO_URL,
    directory_name=MATCHING_DIRECTORY_NAME
)


XML_FILE_CONTENT_ENCODED = 'TWVyaGFiYSBEdW55YSEgSGVsbG8gV29ybGQh'
XML_FILE_CONTENT_DECODED = 'Merhaba Dunya! Hello World!'

XML_FILE_JSON = {
    'content': XML_FILE_CONTENT_ENCODED,
    'encoding': 'base64'
}

XML_FILE_URL_LIST = ['xml_file_url_1']


class TestGetJsonResponseFromUrl:
    def test_should_pass_url_to_request_get_function(
        self,
        requests_mock: MagicMock
    ):
        get_json_response_from_url(url=GIT_REPO_URL)
        requests_mock.get.assert_called_with(url=GIT_REPO_URL)



class TestGetUrlOfXmlFileDirectoryFromRepo:
    def test_should_return_url_for_matching_path(
        self,
        get_json_response_from_url_mock: MagicMock
    ):
        get_json_response_from_url_mock.return_value = GITHUB_TREE_REPONSE_1
        actual_return_value = get_url_of_xml_file_directory_from_repo(SOURCE_CONFIG_1)
        assert actual_return_value == MATCHING_URL


@pytest.mark.usefixtures("get_url_of_xml_file_directory_from_repo_mock")
class TestIterXmlFileUrlFromGitDirectory:
    def test_should_return_url_list_only_for_not_empty_and_non_processed_files(
        self,
        get_json_response_from_url_mock: MagicMock
    ):
        get_json_response_from_url_mock.return_value = GITHUB_TREE_REPONSE_2
        actual_return_value = list(iter_xml_file_url_from_git_directory(
            source_config=SOURCE_CONFIG_1,
            processed_file_url_list=PROCESSED_FILE_URL_LIST
        ))
        assert actual_return_value == [NOT_EMPTY_ARTICLE_URL_1, NOT_EMPTY_ARTICLE_URL_2]


class TestIterDecodedXmlFileContent:
    def test_should_return_decoded_file_content(
        self,
        get_json_response_from_url_mock: MagicMock
    ):
        get_json_response_from_url_mock.return_value = XML_FILE_JSON
        actual_return_value = list(iter_decoded_xml_file_content(XML_FILE_URL_LIST))
        assert actual_return_value == [XML_FILE_CONTENT_DECODED]
