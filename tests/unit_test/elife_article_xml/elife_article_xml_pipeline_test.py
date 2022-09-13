from unittest.mock import MagicMock, patch
import pytest

from data_pipeline.elife_article_xml import (
    elife_article_xml_pipeline as elife_article_xml_pipeline_module
)
from data_pipeline.elife_article_xml.elife_article_xml_config import (
    ElifeArticleXmlConfig,
    ElifeArticleXmlSourceConfig
)

from data_pipeline.elife_article_xml.elife_article_xml_pipeline import (
    GitHubRateLimitError,
    fetch_related_article_from_elife_article_xml_repo_and_load_into_bq,
    get_bq_compatible_json_dict,
    get_json_response_from_url,
    get_url_of_xml_file_directory_from_repo,
    iter_unprocessed_xml_file_url_and_path_from_git_directory,
    iter_xml_file_url_path_and_decoded_content,
    get_article_json_data_from_xml_string_content
)
from data_pipeline.utils.pipeline_config import BigQueryTargetConfig
from tests.unit_test.utils.collections_test import _iter_item_or_raise_exception


@pytest.fixture(name='requests_mock', autouse=True)
def _requests_mock():
    with patch.object(elife_article_xml_pipeline_module, 'requests') as mock:
        yield mock


@pytest.fixture(name='iter_unprocessed_xml_file_url_and_path_from_git_directory_mock')
def _iter_unprocessed_xml_file_url_and_path_from_git_directory_mock():
    with patch.object(
        elife_article_xml_pipeline_module,
        'iter_unprocessed_xml_file_url_and_path_from_git_directory'
    ) as mock:
        yield mock


@pytest.fixture(name='get_json_response_from_url_mock')
def _get_json_response_from_url_mock():
    with patch.object(elife_article_xml_pipeline_module, 'get_json_response_from_url') as mock:
        yield mock


@pytest.fixture(name='iter_xml_file_url_path_and_decoded_content_mock')
def _iter_xml_file_url_path_and_decoded_content_mock():
    with patch.object(
        elife_article_xml_pipeline_module,
        'iter_xml_file_url_path_and_decoded_content'
    ) as mock:
        yield mock


@pytest.fixture(name='get_single_column_value_list_from_bq_query_mock')
def _get_single_column_value_list_from_bq_query_mock():
    with patch.object(
        elife_article_xml_pipeline_module,
        'get_single_column_value_list_from_bq_query'
    ) as mock:
        yield mock


@pytest.fixture(name='get_url_of_xml_file_directory_from_repo_mock')
def _get_url_of_xml_file_directory_from_repo_mock():
    with patch.object(
      elife_article_xml_pipeline_module, 'get_url_of_xml_file_directory_from_repo'
    ) as mock:
        yield mock


@pytest.fixture(name='parse_xml_and_return_it_as_dict_mock')
def _parse_xml_and_return_it_as_dict_mock():
    with patch.object(elife_article_xml_pipeline_module, 'parse_xml_and_return_it_as_dict') as mock:
        yield mock


@pytest.fixture(name='fetch_and_iter_related_article_from_elife_article_xml_repo_mock')
def _fetch_and_iter_related_article_from_elife_article_xml_repomock():
    with patch.object(
        elife_article_xml_pipeline_module,
        'fetch_and_iter_related_article_from_elife_article_xml_repo'
    ) as mock:
        yield mock


@pytest.fixture(name='load_given_json_list_data_from_tempdir_to_bq_mock', autouse=True)
def _load_given_json_list_data_from_tempdir_to_bq_mock():
    with patch.object(
        elife_article_xml_pipeline_module,
        'load_given_json_list_data_from_tempdir_to_bq'
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

EMPTY_ARTICLE_PATH = 'file1-v1.xml'
NOT_EMPTY_ARTICLE_PATH_1 = 'file2-v1.xml'
NOT_EMPTY_ARTICLE_PATH_2 = 'file2-v2.xml'
PROCESSED_NOT_EMPTY_ARTICLE_PATH = 'file3-v1.xml'

PROCESSED_FILE_URL_LIST = [PROCESSED_NOT_EMPTY_ARTICLE_URL]

GITHUB_TREE_REPONSE_2 = {
    'tree': [
        {
            'path': EMPTY_ARTICLE_PATH,
            'size': 0,
            'url': EMPTY_ARTICLE_URL
        },
        {
            'path': NOT_EMPTY_ARTICLE_PATH_1,
            'size': 10,
            'url': NOT_EMPTY_ARTICLE_URL_1
        },
        {
            'path': NOT_EMPTY_ARTICLE_PATH_2,
            'size': 15,
            'url': NOT_EMPTY_ARTICLE_URL_2
        },
        {
            'path': PROCESSED_NOT_EMPTY_ARTICLE_PATH,
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

TARGET_CONFIG_1 = BigQueryTargetConfig(
    project_name='project1',
    dataset_name='dataset1',
    table_name='table1'
)

CONFIG_1 = ElifeArticleXmlConfig(
    source=SOURCE_CONFIG_1,
    target=TARGET_CONFIG_1
)


XML_FILE_CONTENT_ENCODED = 'TWVyaGFiYSBEdW55YSEgSGVsbG8gV29ybGQh'
XML_FILE_CONTENT_DECODED = 'Merhaba Dunya! Hello World!'

XML_FILE_JSON = {
    'content': XML_FILE_CONTENT_ENCODED,
    'encoding': 'base64'
}

XML_FILE_URL_AND_PATH_LIST = [('xml_file_url_1', 'xml_file_path_1')]

ARTICLE_TYPE_1 = 'article_type_1'
ARTICLE_ID_1 = 'article_id_1'
ARTICLE_DOI_1 = 'article_doi_1'

RELATED_ARTICLE_TYPE_1 = 'related_article_type_1'
RELATED_ARTICLE_DOI_1 = 'related_article_doi_1'

RELATED_ARTICLE_LIST = [
    {
        'related-article-type': RELATED_ARTICLE_TYPE_1,
        '{http://www.w3.org/1999/xlink}href': RELATED_ARTICLE_DOI_1
    }
]

XML_FILE_CONTENT_DICT_WITH_RELATED_ARTICLE_SECTION = {
  'article': {
    'article-type': ARTICLE_TYPE_1,
    'front': [
      {
        'article-meta': [
          {
            'article-id': [
              {
                'pub-id-type': 'publisher-id',
                '_text': ARTICLE_ID_1
              },
              {
                'pub-id-type': 'doi',
                '_text': ARTICLE_DOI_1
              }
            ],
            'related-article': RELATED_ARTICLE_LIST
          }
        ]
      }
    ]
  }
}


class TestGetJsonResponseFromUrl:
    def test_should_pass_url_to_request_get_function(
        self,
        requests_mock: MagicMock
    ):
        get_json_response_from_url(url=GIT_REPO_URL, headers={})
        requests_mock.get.assert_called_with(url=GIT_REPO_URL, headers={})

    def test_should_raise_rate_limit_error_if_status_code_was_403(
        self,
        requests_mock: MagicMock
    ):
        response_mock = requests_mock.get.return_value
        response_mock.status_code = 403
        response_mock.raise_for_status.side_effect = RuntimeError()
        with pytest.raises(GitHubRateLimitError):
            get_json_response_from_url(url=GIT_REPO_URL)

    def test_should_raise_exceptions_from_raise_for_status(
        self,
        requests_mock: MagicMock
    ):
        response_mock = requests_mock.get.return_value
        response_mock.raise_for_status.side_effect = RuntimeError()
        with pytest.raises(RuntimeError):
            get_json_response_from_url(url=GIT_REPO_URL)


class TestGetUrlOfXmlFileDirectoryFromRepo:
    def test_should_return_url_for_matching_path(
        self,
        get_json_response_from_url_mock: MagicMock
    ):
        get_json_response_from_url_mock.return_value = GITHUB_TREE_REPONSE_1
        actual_return_value = get_url_of_xml_file_directory_from_repo(SOURCE_CONFIG_1)
        assert actual_return_value == MATCHING_URL


@pytest.mark.usefixtures("get_url_of_xml_file_directory_from_repo_mock")
class TestIterXmlFileUrlAndPathFromGitDirectory:
    def test_should_return_url_and_path_list_only_for_not_empty_and_non_processed_files(
        self,
        get_json_response_from_url_mock: MagicMock
    ):
        get_json_response_from_url_mock.return_value = GITHUB_TREE_REPONSE_2
        actual_return_value = list(iter_unprocessed_xml_file_url_and_path_from_git_directory(
            source_config=SOURCE_CONFIG_1,
            processed_file_url_list=PROCESSED_FILE_URL_LIST
        ))
        assert actual_return_value == [
            (NOT_EMPTY_ARTICLE_URL_1, NOT_EMPTY_ARTICLE_PATH_1),
            (NOT_EMPTY_ARTICLE_URL_2, NOT_EMPTY_ARTICLE_PATH_2)
        ]

    def test_should_return_all_url_and_path_list_if_processed_files_list_empty(
        self,
        get_json_response_from_url_mock: MagicMock
    ):
        get_json_response_from_url_mock.return_value = GITHUB_TREE_REPONSE_2
        actual_return_value = list(iter_unprocessed_xml_file_url_and_path_from_git_directory(
            source_config=SOURCE_CONFIG_1,
            processed_file_url_list=[]
        ))
        assert actual_return_value == [
            (NOT_EMPTY_ARTICLE_URL_1, NOT_EMPTY_ARTICLE_PATH_1),
            (NOT_EMPTY_ARTICLE_URL_2, NOT_EMPTY_ARTICLE_PATH_2),
            (PROCESSED_NOT_EMPTY_ARTICLE_URL, PROCESSED_NOT_EMPTY_ARTICLE_PATH)
        ]


class TestIterXmlFileUrlPathAndDecodedContent:
    def test_should_return_file_url_file_path_and_its_decoded_content(
        self,
        get_json_response_from_url_mock: MagicMock
    ):
        get_json_response_from_url_mock.return_value = XML_FILE_JSON
        actual_return_value = list(
            iter_xml_file_url_path_and_decoded_content(SOURCE_CONFIG_1, XML_FILE_URL_AND_PATH_LIST)
        )
        assert actual_return_value == [
            ('xml_file_url_1', 'xml_file_path_1', XML_FILE_CONTENT_DECODED)
        ]


class TestIterBqCompatibleJson:
    def test_should_keep_compatible_key_name_with_underscore(self):
        input_data = [{
            'key_1': 'value1'
        }]
        result = get_bq_compatible_json_dict(input_data)
        assert result == input_data

    def test_should_replace_hyphen_in_key_name_with_underscore(self):
        input_data = [{
            'key-1': 'value1'
        }]
        expected_data = [{
            'key_1': 'value1'
        }]
        result = get_bq_compatible_json_dict(input_data)
        assert result == expected_data

    def test_should_remove_none_value(self):
        input_data = [{
            'key_1': None
        }]
        expected_data = []
        result = get_bq_compatible_json_dict(input_data)
        assert result == expected_data

    def test_should_remove_empty_list_value(self):
        input_data = [{
            'key_1': []
        }]
        expected_data = []
        result = get_bq_compatible_json_dict(input_data)
        assert result == expected_data


class TestGetArticleJsonDataFromXmlStringContent:
    def test_should_return_empty_dict_if_the_xml_with_empty_root(self):
        xml_string = '<article></article>'
        return_value = get_article_json_data_from_xml_string_content(xml_string)
        assert return_value == {}

    def test_should_return_empty_dict_if_the_xml_has_empty_article_meta(self):
        xml_string = '<article><front><article-meta></article-meta></front></article>'
        return_value = get_article_json_data_from_xml_string_content(xml_string)
        assert return_value == {}

    def test_should_not_return_empty_dict_if_the_xml_has_no_article_meta_section(self):
        xml_string = ''.join([
            '<article article-type="article_type_1" xmlns:xlink="http://www.w3.org/1999/xlink">',
            '<front>',
            '<other2>"other2"</other2>',
            '</front>',
            '</article>'
        ])
        return_value = get_article_json_data_from_xml_string_content(xml_string)
        assert return_value == {}

    def test_should_return_empty_dict_if_the_xml_has_no_front_section(self):
        xml_string = ''.join([
            '<article article-type="article_type_1" xmlns:xlink="http://www.w3.org/1999/xlink">',
            '<other2>"other2"</other2>',
            '</article>'
        ])
        return_value = get_article_json_data_from_xml_string_content(xml_string)
        assert return_value == {}

    def test_should_return_related_article_article_id_type_and_categories_dict_if_they_exist(self):
        xml_string = ''.join([
            '<article article-type="article_type_1" xmlns:xlink="http://www.w3.org/1999/xlink">',
            '<front><article-meta>',
            '<article-id pub-id-type="publisher-id">publisher_id_1</article-id>',
            '<article-id pub-id-type="doi">doi_1</article-id>',
            '<article-categories>',
            '<subj-group subj-group-type="subj_group_type_1"><subject>subject_1</subject>',
            '</subj-group><subj-group subj-group-type="subj_group_type_2">',
            '<subject>subject_2</subject></subj-group>',
            '</article-categories>',
            '<related-article ext-link-type="doi" id="ra1" ',
            'related-article-type="related_article_type_1" xlink:href="related_article_doi_1"/>',
            '</article-meta></front>',
            '</article>'
        ])

        return_value = get_article_json_data_from_xml_string_content(xml_string)
        assert return_value == {
            'article_type': 'article_type_1',
            'article_id': [
                {
                    'pub_id_type': 'publisher-id',
                    'value_text': 'publisher_id_1'
                },
                {
                    'pub_id_type': 'doi',
                    'value_text': 'doi_1'
                }
            ],
            'article_categories': [{'subj_group': [{
                'subj_group_type': 'subj_group_type_1',
                'subject': [{'value_text': 'subject_1'}]
            }, {
                'subj_group_type': 'subj_group_type_2',
                'subject': [{'value_text': 'subject_2'}]
            }]}],
            'related_article': [{
                'ext_link_type': 'doi',
                'id': 'ra1',
                'related_article_type': 'related_article_type_1',
                'href': 'related_article_doi_1'
            }]
        }

    def test_should_not_return_other_elements(self):
        xml_string = ''.join([
            '<article article-type="article_type_1" xmlns:xlink="http://www.w3.org/1999/xlink">',
            '<front><article-meta>',
            '<article-id pub-id-type="publisher-id">publisher_id_1</article-id>',
            '<article-id pub-id-type="doi">doi_1</article-id>',
            '<article-categories>',
            '<subj-group subj-group-type="subj_group_type_1"><subject>subject_1</subject>',
            '</subj-group><subj-group subj-group-type="subj_group_type_2">',
            '<subject>subject_2</subject></subj-group>',
            '</article-categories>',
            '<related-article ext-link-type="doi" id="ra1" ',
            'related-article-type="related_article_type_1" xlink:href="related_article_doi_1"/>',
            '<other>"other"</other>',
            '</article-meta>',
            '<other2>"other2"</other2>',
            '</front>',
            '<other3>"other3"</other3>',
            '</article>'
        ])
        return_value = get_article_json_data_from_xml_string_content(xml_string)
        assert return_value == {
            'article_type': 'article_type_1',
            'article_id': [
                {
                    'pub_id_type': 'publisher-id',
                    'value_text': 'publisher_id_1'
                },
                {
                    'pub_id_type': 'doi',
                    'value_text': 'doi_1'
                }
            ],
            'article_categories': [{'subj_group': [{
                'subj_group_type': 'subj_group_type_1',
                'subject': [{'value_text': 'subject_1'}]
            }, {
                'subj_group_type': 'subj_group_type_2',
                'subject': [{'value_text': 'subject_2'}]
            }]}],
            'related_article': [{
                'ext_link_type': 'doi',
                'id': 'ra1',
                'related_article_type': 'related_article_type_1',
                'href': 'related_article_doi_1'
            }]
        }

    def test_should_return_dict_with_all_related_article_if_there_are_more_than_one(self):
        xml_string = ''.join([
            '<article article-type="article_type_1" xmlns:xlink="http://www.w3.org/1999/xlink">',
            '<front><article-meta>',
            '<article-id pub-id-type="publisher-id">publisher_id_1</article-id>',
            '<article-id pub-id-type="doi">doi_1</article-id>',
            '<related-article ext-link-type="doi_1" id="ra1" ',
            'related-article-type="related_article_type_1" xlink:href="related_article_doi_1"/>',
            '<related-article ext-link-type="doi_2" id="ra2" ',
            'related-article-type="related_article_type_2" xlink:href="related_article_doi_2"/>',
            '</article-meta></front>',
            '</article>'
        ])

        return_value = get_article_json_data_from_xml_string_content(xml_string)
        assert return_value == {
            'article_type': 'article_type_1',
            'article_id': [
                {
                    'pub_id_type': 'publisher-id',
                    'value_text': 'publisher_id_1'
                },
                {
                    'pub_id_type': 'doi',
                    'value_text': 'doi_1'
                }
            ],
            'related_article': [
                {
                    'ext_link_type': 'doi_1',
                    'id': 'ra1',
                    'related_article_type': 'related_article_type_1',
                    'href': 'related_article_doi_1'
                },
                {
                    'ext_link_type': 'doi_2',
                    'id': 'ra2',
                    'related_article_type': 'related_article_type_2',
                    'href': 'related_article_doi_2'
                }
            ]
        }


class TestFetchRelatedArticleFromElifeArticleXmlRepoAndLoadIntoBq:
    def test_should_load_data_until_rate_limit_exception(
        self,
        load_given_json_list_data_from_tempdir_to_bq_mock: MagicMock,
        fetch_and_iter_related_article_from_elife_article_xml_repo_mock: MagicMock
    ):
        fetch_and_iter_related_article_from_elife_article_xml_repo_mock.return_value = (
            _iter_item_or_raise_exception([1, 2, 3, GitHubRateLimitError()])
        )

        fetch_related_article_from_elife_article_xml_repo_and_load_into_bq(CONFIG_1)

        _args, kwargs = load_given_json_list_data_from_tempdir_to_bq_mock.call_args
        assert list(kwargs['json_list']) == [1, 2, 3]
