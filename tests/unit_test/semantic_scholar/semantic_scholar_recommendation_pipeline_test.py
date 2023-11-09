from datetime import datetime
from unittest.mock import ANY, MagicMock, patch

import pytest
from data_pipeline.semantic_scholar.semantic_scholar_recommendation_config import (
    SemanticScholarRecommendationConfig
)
from data_pipeline.utils.pipeline_config import (
    BigQueryIncludeExcludeSourceConfig,
    BigQuerySourceConfig,
    BigQueryTargetConfig,
    BigQueryWrappedExcludeSourceConfig,
    BigQueryWrappedSourceConfig,
    MappingConfig
)
from data_pipeline.semantic_scholar.semantic_scholar_config import (
    SemanticScholarMatrixConfig,
    SemanticScholarSourceConfig
)
from data_pipeline.semantic_scholar.semantic_scholar_pipeline import get_progress_message
from data_pipeline.semantic_scholar import (
    semantic_scholar_recommendation_pipeline as semantic_scholar_recommendation_pipeline_module
)
from data_pipeline.semantic_scholar.semantic_scholar_recommendation_pipeline import (
    ExcludableListItem,
    ExcludableListWithMeta,
    fetch_article_data_from_semantic_scholar_recommendation_and_load_into_bigquery,
    get_ordered_doi_list_for_item_list,
    get_recommendation_response_json_from_api,
    iter_list_for_matrix_config,
    iter_list_with_positive_items,
    iter_recommendation_data
)


DOI_1 = 'doi1'
DOI_2 = 'doi2'

DOI_LIST = [DOI_1, DOI_2]


LIST_1 = {'list_key': 'key1'}

LIST_ITEM_1 = {
    'doi': DOI_1,
    'is_excluded': False,
    'item_timestamp': datetime.fromisoformat('2021-01-01T01:01:01')
}


ITEM_RESPONSE_JSON_1 = {
    'externalIds': {
        'DOI': DOI_1
    }
}


PROVENANCE_1 = {'provenance1': 'value1'}


BIGQUERY_SOURCE_CONFIG_1 = BigQuerySourceConfig(
    project_name='project1',
    sql_query='query1'
)


BIGQUERY_SOURCE_CONFIG_2 = BigQuerySourceConfig(
    project_name='project2',
    sql_query='query2'
)


MATRIX_VARIABLE_CONFIG_1 = BigQueryIncludeExcludeSourceConfig(
    include=BigQueryWrappedSourceConfig(
        bigquery=BIGQUERY_SOURCE_CONFIG_1
    )
)


MATRIX_VARIABLE_WITH_EXCLUDE_CONFIG_1 = BigQueryIncludeExcludeSourceConfig(
    include=BigQueryWrappedSourceConfig(
        bigquery=BIGQUERY_SOURCE_CONFIG_1
    ),
    exclude=BigQueryWrappedExcludeSourceConfig(
        bigquery=BIGQUERY_SOURCE_CONFIG_2,
        key_field_name='list_key'
    )
)


MATRIX_CONFIG_1 = SemanticScholarMatrixConfig(
    variables={
        'list': MATRIX_VARIABLE_CONFIG_1
    }
)


MATRIX_WITH_EXCLUDE_CONFIG_1 = SemanticScholarMatrixConfig(
    variables={
        'list': MATRIX_VARIABLE_WITH_EXCLUDE_CONFIG_1
    }
)


SOURCE_CONFIG_1 = SemanticScholarSourceConfig(
    api_url='/api1/recommendation',
    params={'param1': 'value1'}
)


TARGET_CONFIG_1 = BigQueryTargetConfig(
    project_name='project1',
    dataset_name='dataset1',
    table_name='table1'
)


CONFIG_1 = SemanticScholarRecommendationConfig(
    matrix=MATRIX_CONFIG_1,
    source=SOURCE_CONFIG_1,
    target=TARGET_CONFIG_1
)


MOCK_UTC_NOW_STR = '2021-01-02T12:34:56'


@pytest.fixture(name='datetime_mock', autouse=True)
def _datetime_mock():
    with patch.object(semantic_scholar_recommendation_pipeline_module, 'datetime') as mock:
        mock.utcnow.return_value = datetime.fromisoformat(MOCK_UTC_NOW_STR)
        yield mock


@pytest.fixture(name='requests_retry_session_mock', autouse=True)
def _requests_retry_session_mock():
    with patch.object(
        semantic_scholar_recommendation_pipeline_module,
        'requests_retry_session'
    ) as mock:
        yield mock


@pytest.fixture(name='session_mock')
def _session_mock(requests_retry_session_mock: MagicMock) -> MagicMock:
    return requests_retry_session_mock.return_value.__enter__.return_value


@pytest.fixture(name='load_given_json_list_data_from_tempdir_to_bq_mock', autouse=True)
def _load_given_json_list_data_from_tempdir_to_bq_mock():
    with patch.object(
        semantic_scholar_recommendation_pipeline_module,
        'load_given_json_list_data_from_tempdir_to_bq'
    ) as mock:
        yield mock


@pytest.fixture(name='iter_dict_for_bigquery_source_config_with_exclusion_mock', autouse=True)
def _iter_dict_for_bigquery_source_config_with_exclusion_mock():
    with patch.object(
        semantic_scholar_recommendation_pipeline_module,
        'iter_dict_for_bigquery_source_config_with_exclusion'
    ) as mock:
        yield mock


@pytest.fixture(name='get_response_json_with_provenance_from_api_mock', autouse=True)
def _get_response_json_with_provenance_from_api_mock():
    with patch.object(
        semantic_scholar_recommendation_pipeline_module,
        'get_response_json_with_provenance_from_api'
    ) as mock:
        yield mock


@pytest.fixture(name='iter_list_for_matrix_config_mock')
def _iter_list_for_matrix_config_mock():
    with patch.object(
        semantic_scholar_recommendation_pipeline_module,
        'iter_list_for_matrix_config'
    ) as mock:
        yield mock


@pytest.fixture(name='iter_list_with_positive_items_mock')
def _iter_list_with_positive_items_mock():
    with patch.object(
        semantic_scholar_recommendation_pipeline_module,
        'iter_list_with_positive_items'
    ) as mock:
        yield mock


@pytest.fixture(name='get_recommendation_response_json_from_api_mock')
def _get_recommendation_response_json_from_api_mock():
    with patch.object(
        semantic_scholar_recommendation_pipeline_module,
        'get_recommendation_response_json_from_api'
    ) as mock:
        yield mock


@pytest.fixture(name='iter_recommendation_data_mock')
def _iter_recommendation_data_mock():
    with patch.object(
        semantic_scholar_recommendation_pipeline_module,
        'iter_recommendation_data'
    ) as mock:
        yield mock


class TestIterListForMatrixConfig:
    def test_should_call_iter_dict_from_bq_query_without_exclusion(
        self,
        iter_dict_for_bigquery_source_config_with_exclusion_mock: MagicMock
    ):
        iter_list_for_matrix_config(MATRIX_CONFIG_1)
        iter_dict_for_bigquery_source_config_with_exclusion_mock.assert_called_with(
            MATRIX_VARIABLE_CONFIG_1.include.bigquery,
            key_field_name='list_key',
            exclude_bigquery_source_config=None
        )

    def test_should_call_iter_dict_from_bq_query_with_exclusion(
        self,
        iter_dict_for_bigquery_source_config_with_exclusion_mock: MagicMock
    ):
        iter_list_for_matrix_config(MATRIX_WITH_EXCLUDE_CONFIG_1)
        assert MATRIX_VARIABLE_WITH_EXCLUDE_CONFIG_1.exclude is not None
        iter_dict_for_bigquery_source_config_with_exclusion_mock.assert_called_with(
            MATRIX_VARIABLE_WITH_EXCLUDE_CONFIG_1.include.bigquery,
            key_field_name='list_key',
            exclude_bigquery_source_config=MATRIX_VARIABLE_WITH_EXCLUDE_CONFIG_1.exclude.bigquery
        )

    def test_should_return_parsed_include_list(
        self,
        iter_dict_for_bigquery_source_config_with_exclusion_mock: MagicMock
    ):
        iter_dict_for_bigquery_source_config_with_exclusion_mock.return_value = [
            {
                'list_key': 'list1',
                'list_meta': {'meta_key1': 'meta_value1'},
                'list': [
                    {'doi': DOI_1, 'is_excluded': False, 'other_key': 'other_value'}
                ]
            }
        ]
        result = list(iter_list_for_matrix_config(MATRIX_CONFIG_1))
        assert result == [
            ExcludableListWithMeta(
                list_key='list1',
                list_meta={'meta_key1': 'meta_value1'},
                item_list=[
                    ExcludableListItem(
                        doi=DOI_1,
                        is_excluded=False,
                        json_data={'doi': DOI_1, 'is_excluded': False, 'other_key': 'other_value'}
                    )
                ]
            )
        ]


class TestGetOrderPreservingDoiListByEvents:
    def test_should_return_included_dois(self):
        assert get_ordered_doi_list_for_item_list(
            [
                ExcludableListItem(DOI_1),
                ExcludableListItem(DOI_2)
            ]
        ) == [DOI_1, DOI_2]

    def test_should_excluded_doi(self):
        assert get_ordered_doi_list_for_item_list(
            [
                ExcludableListItem(DOI_1),
                ExcludableListItem(DOI_1, is_excluded=True),
                ExcludableListItem(DOI_2)
            ]
        ) == [DOI_2]

    def test_should_return_excluded_dois(self):
        assert get_ordered_doi_list_for_item_list(
            [
                ExcludableListItem(DOI_1),
                ExcludableListItem(DOI_1, is_excluded=True),
                ExcludableListItem(DOI_2)
            ],
            return_exclude_list=True
        ) == [DOI_1]


class TestIterListWithPositiveItems:
    def test_should_include_list_with_positive_dois(self):
        list_with_positive_dois = ExcludableListWithMeta(list_key='key1', item_list=[
            ExcludableListItem(doi=DOI_1)
        ])
        assert list(iter_list_with_positive_items(
            [list_with_positive_dois]
        )) == [list_with_positive_dois]

    def test_should_exclude_list_with_positive_dois(self):
        list_with_positive_dois = ExcludableListWithMeta(list_key='key1', item_list=[
            ExcludableListItem(doi=DOI_1),
            ExcludableListItem(doi=DOI_1, is_excluded=True)
        ])
        assert not list(iter_list_with_positive_items(
            [list_with_positive_dois]
        ))


class TestGetRecommendationResponseJsonFromApi:
    def test_should_pass_api_url_and_params_to_api(
        self,
        session_mock: MagicMock,
        get_response_json_with_provenance_from_api_mock: MagicMock
    ):
        get_recommendation_response_json_from_api(
            ExcludableListWithMeta(list_key='key1', item_list=[
                ExcludableListItem(doi=DOI_1)
            ]),
            source_config=SOURCE_CONFIG_1,
            provenance=None,
            session=session_mock,
            progress_message='progress1'
        )
        get_response_json_with_provenance_from_api_mock.assert_called_with(
            SOURCE_CONFIG_1.api_url,
            params=SOURCE_CONFIG_1.params,
            headers=ANY,
            printable_headers=ANY,
            method='POST',
            json_data=ANY,
            provenance=ANY,
            session=session_mock,
            raise_on_status=False,
            progress_message='progress1'
        )

    def test_should_pass_headers_and_pritable_headers(
        self,
        get_response_json_with_provenance_from_api_mock: MagicMock
    ):
        headers_config = MappingConfig(
            mapping={'header1': 'value1'},
            printable_mapping={'header1': '***'}
        )
        get_recommendation_response_json_from_api(
            ExcludableListWithMeta(list_key='key1', item_list=[
                ExcludableListItem(doi=DOI_1)
            ]),
            source_config=SOURCE_CONFIG_1._replace(headers=headers_config)
        )
        _args, kwargs = get_response_json_with_provenance_from_api_mock.call_args
        assert kwargs['headers'] == headers_config.mapping
        assert kwargs['printable_headers'] == headers_config.printable_mapping

    def test_should_pass_positive_and_negative_paper_ids_to_api(
        self,
        session_mock: MagicMock,
        get_response_json_with_provenance_from_api_mock: MagicMock
    ):
        get_recommendation_response_json_from_api(
            ExcludableListWithMeta(list_key='key1', item_list=[
                ExcludableListItem(doi=DOI_1),
                ExcludableListItem(doi=DOI_1, is_excluded=True),
                ExcludableListItem(doi=DOI_2)
            ]),
            source_config=SOURCE_CONFIG_1,
            provenance=None,
            session=session_mock,
            progress_message='progress1'
        )
        _, kwargs = get_response_json_with_provenance_from_api_mock.call_args
        assert kwargs['json_data'] == {
            'positivePaperIds': [f'DOI:{DOI_2}'],
            'negativePaperIds': [f'DOI:{DOI_1}']
        }

    def test_should_pass_last_n_positive_and_negative_paper_ids_to_api(
        self,
        session_mock: MagicMock,
        get_response_json_with_provenance_from_api_mock: MagicMock
    ):
        item_list = [
            ExcludableListItem(doi='pos_1'),
            ExcludableListItem(doi='neg_1'),
            ExcludableListItem(doi='neg_1', is_excluded=True),
            ExcludableListItem(doi='pos_2'),
            ExcludableListItem(doi='neg_2'),
            ExcludableListItem(doi='neg_2', is_excluded=True),
            ExcludableListItem(doi='pos_3'),
            ExcludableListItem(doi='neg_3'),
            ExcludableListItem(doi='neg_3', is_excluded=True),
        ]
        get_recommendation_response_json_from_api(
            ExcludableListWithMeta(list_key='key1', item_list=item_list),
            source_config=SOURCE_CONFIG_1,
            provenance=None,
            session=session_mock,
            progress_message='progress1',
            max_paper_ids=2
        )
        _, kwargs = get_response_json_with_provenance_from_api_mock.call_args
        assert kwargs['json_data'] == {
            'positivePaperIds': ['DOI:pos_2', 'DOI:pos_3'],
            'negativePaperIds': ['DOI:neg_2', 'DOI:neg_3']
        }

    def test_should_add_list_meta_to_provenance(
        self,
        session_mock: MagicMock,
        get_response_json_with_provenance_from_api_mock: MagicMock
    ):
        get_recommendation_response_json_from_api(
            ExcludableListWithMeta(
                list_key='key1',
                list_meta={'meta_key1': 'meta_value1'},
                item_list=[ExcludableListItem(doi=DOI_1, json_data=LIST_ITEM_1)]
            ),
            source_config=SOURCE_CONFIG_1,
            provenance=None,
            session=session_mock,
            progress_message='progress1'
        )
        _, kwargs = get_response_json_with_provenance_from_api_mock.call_args
        assert kwargs['provenance']['list_key'] == 'key1'
        assert kwargs['provenance']['list_meta'] == {'meta_key1': 'meta_value1'}
        assert kwargs['provenance']['item_list'] == [LIST_ITEM_1]


class TestIterRecommendationData:
    def test_should_call_fetch_article_by_doi_with_doi(
        self,
        session_mock: MagicMock,
        get_recommendation_response_json_from_api_mock: MagicMock
    ):
        list_iterable = iter([LIST_1])
        result = list(iter_recommendation_data(
            list_iterable,
            source_config=SOURCE_CONFIG_1,
            provenance=PROVENANCE_1,
            session=session_mock
        ))
        assert result == [get_recommendation_response_json_from_api_mock.return_value]
        get_recommendation_response_json_from_api_mock.assert_called_with(
            LIST_1,
            source_config=SOURCE_CONFIG_1,
            provenance=PROVENANCE_1,
            session=session_mock,
            progress_message=get_progress_message(0, list_iterable)
        )


class TestFetchArticleDataFromSemanticScholarRecommendationAndLoadIntoBigQuery:
    def test_should_pass_provenance_to_iter_recommendation_data(
        self,
        iter_recommendation_data_mock: MagicMock
    ):
        json_list = [ITEM_RESPONSE_JSON_1]
        iter_recommendation_data_mock.return_value = json_list
        fetch_article_data_from_semantic_scholar_recommendation_and_load_into_bigquery(
            CONFIG_1
        )
        iter_recommendation_data_mock.assert_called_with(
            ANY,
            source_config=ANY,
            provenance={'imported_timestamp': MOCK_UTC_NOW_STR},
            session=ANY
        )

    def test_should_pass_iter_doi_result_to_iter_recommendation_data(
        self,
        iter_list_for_matrix_config_mock: MagicMock,
        iter_list_with_positive_items_mock: MagicMock,
        iter_recommendation_data_mock: MagicMock
    ):
        json_list = [ITEM_RESPONSE_JSON_1]
        iter_recommendation_data_mock.return_value = json_list
        fetch_article_data_from_semantic_scholar_recommendation_and_load_into_bigquery(
            CONFIG_1
        )
        iter_list_for_matrix_config_mock.assert_called_with(
            MATRIX_CONFIG_1
        )
        iter_list_with_positive_items_mock.assert_called_with(
            iter_list_for_matrix_config_mock.return_value
        )
        iter_recommendation_data_mock.assert_called_with(
            iter_list_with_positive_items_mock.return_value,
            source_config=SOURCE_CONFIG_1,
            provenance=ANY,
            session=ANY
        )

    def test_should_pass_retry_session_to_iter_recommendation_data(
        self,
        requests_retry_session_mock: MagicMock,
        session_mock: MagicMock,
        iter_recommendation_data_mock: MagicMock
    ):
        fetch_article_data_from_semantic_scholar_recommendation_and_load_into_bigquery(
            CONFIG_1
        )
        requests_retry_session_mock.assert_called()
        iter_recommendation_data_mock.assert_called_with(
            ANY,
            source_config=SOURCE_CONFIG_1,
            provenance=ANY,
            session=session_mock
        )

    def test_should_pass_project_dataset_and_table_to_bq_load_method(
        self,
        iter_recommendation_data_mock: MagicMock,
        load_given_json_list_data_from_tempdir_to_bq_mock: MagicMock
    ):
        json_list = [ITEM_RESPONSE_JSON_1]
        iter_recommendation_data_mock.return_value = json_list
        fetch_article_data_from_semantic_scholar_recommendation_and_load_into_bigquery(
            CONFIG_1
        )
        load_given_json_list_data_from_tempdir_to_bq_mock.assert_called()
        load_given_json_list_data_from_tempdir_to_bq_mock.assert_called_with(
            project_name=TARGET_CONFIG_1.project_name,
            dataset_name=TARGET_CONFIG_1.dataset_name,
            table_name=TARGET_CONFIG_1.table_name,
            json_list=ANY
        )
