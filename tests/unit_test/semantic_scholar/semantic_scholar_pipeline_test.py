from datetime import datetime
from typing import Optional
from unittest.mock import ANY, MagicMock, patch

import pytest

from data_pipeline.utils.pipeline_config import (
    BigQueryIncludeExcludeSourceConfig,
    BigQuerySourceConfig,
    BigQueryTargetConfig,
    BigQueryWrappedExcludeSourceConfig,
    BigQueryWrappedSourceConfig,
    MappingConfig
)

from data_pipeline.semantic_scholar.semantic_scholar_config import (
    SemanticScholarConfig,
    SemanticScholarMatrixConfig,
    SemanticScholarSourceConfig
)

from data_pipeline.semantic_scholar import (
    semantic_scholar_pipeline as semantic_scholar_pipeline_module
)
from data_pipeline.semantic_scholar.semantic_scholar_pipeline import (
    get_article_response_json_from_api,
    fetch_article_data_from_semantic_scholar_and_load_into_bigquery,
    get_progress_message,
    get_resolved_api_url,
    iter_article_data,
    iter_doi_for_matrix_config
)


DOI_1 = 'doi1'
DOI_2 = 'doi2'

DOI_LIST = [DOI_1, DOI_2]

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


DOI_MATRIX_VARIABLE_CONFIG_1 = BigQueryIncludeExcludeSourceConfig(
    include=BigQueryWrappedSourceConfig(
        bigquery=BIGQUERY_SOURCE_CONFIG_1
    )
)


DOI_MATRIX_VARIABLE_WITH_EXCLUDE_CONFIG_1 = BigQueryIncludeExcludeSourceConfig(
    include=BigQueryWrappedSourceConfig(
        bigquery=BIGQUERY_SOURCE_CONFIG_1
    ),
    exclude=BigQueryWrappedExcludeSourceConfig(
        bigquery=BIGQUERY_SOURCE_CONFIG_2,
        key_field_name_from_include='doi'
    )
)


MATRIX_CONFIG_1 = SemanticScholarMatrixConfig(
    variables={
        'doi': DOI_MATRIX_VARIABLE_CONFIG_1
    }
)


SOURCE_CONFIG_1 = SemanticScholarSourceConfig(
    api_url='/api1/{doi}',
    params={'param1': 'value1'}
)


TARGET_CONFIG_1 = BigQueryTargetConfig(
    project_name='project1',
    dataset_name='dataset1',
    table_name='table1'
)


CONFIG_1 = SemanticScholarConfig(
    matrix=MATRIX_CONFIG_1,
    source=SOURCE_CONFIG_1,
    target=TARGET_CONFIG_1
)


MOCK_UTC_NOW_STR = '2021-01-02T12:34:56'


@pytest.fixture(name='datetime_mock', autouse=True)
def _datetime_mock():
    with patch.object(semantic_scholar_pipeline_module, 'datetime') as mock:
        mock.utcnow.return_value = datetime.fromisoformat(MOCK_UTC_NOW_STR)
        yield mock


@pytest.fixture(name='requests_retry_session_mock', autouse=True)
def _requests_retry_session_mock():
    with patch.object(
        semantic_scholar_pipeline_module,
        'requests_retry_session'
    ) as mock:
        yield mock


@pytest.fixture(name='session_mock')
def _session_mock(requests_retry_session_mock: MagicMock) -> MagicMock:
    return requests_retry_session_mock.return_value.__enter__.return_value


@pytest.fixture(name='load_given_json_list_data_from_tempdir_to_bq_mock', autouse=True)
def _load_given_json_list_data_from_tempdir_to_bq_mock():
    with patch.object(
        semantic_scholar_pipeline_module,
        'load_given_json_list_data_from_tempdir_to_bq'
    ) as mock:
        yield mock


@pytest.fixture(name='fetch_single_column_value_list_for_bigquery_source_config_mock', autouse=True)
def _fetch_single_column_value_list_for_bigquery_source_config_mock():
    with patch.object(
        semantic_scholar_pipeline_module,
        'fetch_single_column_value_list_for_bigquery_source_config'
    ) as mock:
        yield mock


@pytest.fixture(name='get_response_json_with_provenance_from_api_mock', autouse=True)
def _get_response_json_with_provenance_from_api_mock():
    with patch.object(
        semantic_scholar_pipeline_module,
        'get_response_json_with_provenance_from_api'
    ) as mock:
        yield mock


@pytest.fixture(name='iter_doi_for_matrix_config_mock')
def _iter_doi_for_matrix_config_mock():
    with patch.object(semantic_scholar_pipeline_module, 'iter_doi_for_matrix_config') as mock:
        yield mock


@pytest.fixture(name='get_article_response_json_from_api_mock')
def _get_article_response_json_from_api_mock():
    with patch.object(
        semantic_scholar_pipeline_module,
        'get_article_response_json_from_api'
    ) as mock:
        yield mock


@pytest.fixture(name='iter_article_data_mock')
def _iter_article_data_mock():
    with patch.object(semantic_scholar_pipeline_module, 'iter_article_data') as mock:
        yield mock


class TestIterDoiForMatrixConfig:
    def test_should_call_get_single_column_value_list_from_bq_query(
        self,
        fetch_single_column_value_list_for_bigquery_source_config_mock: MagicMock
    ):
        iter_doi_for_matrix_config(MATRIX_CONFIG_1)
        fetch_single_column_value_list_for_bigquery_source_config_mock.assert_called_with(
            DOI_MATRIX_VARIABLE_CONFIG_1.include.bigquery
        )

    def test_should_return_include_list(
        self,
        fetch_single_column_value_list_for_bigquery_source_config_mock: MagicMock
    ):
        fetch_single_column_value_list_for_bigquery_source_config_mock.return_value = DOI_LIST
        result = list(iter_doi_for_matrix_config(MATRIX_CONFIG_1))
        assert result == DOI_LIST

    def test_should_remove_excluded_values_from_list(
        self,
        fetch_single_column_value_list_for_bigquery_source_config_mock: MagicMock
    ):
        fetch_single_column_value_list_for_bigquery_source_config_mock.side_effect = [
            [DOI_1, DOI_2],  # include
            [DOI_1]  # exclude
        ]
        result = iter_doi_for_matrix_config(MATRIX_CONFIG_1._replace(
            variables={
                'doi': DOI_MATRIX_VARIABLE_WITH_EXCLUDE_CONFIG_1
            }
        ))
        assert result == [DOI_2]


class TestGetResolvedApiUrl:
    def test_should_replace_doi_placeholder(self):
        assert get_resolved_api_url('/api/{doi}', doi=DOI_1) == f'/api/{DOI_1}'


class TestGetArticleResponseJsonFromApi:
    def test_should_pass_resolved_api_url_and_params(
        self,
        session_mock: MagicMock,
        get_response_json_with_provenance_from_api_mock: MagicMock
    ):
        get_article_response_json_from_api(
            DOI_1,
            source_config=SOURCE_CONFIG_1,
            provenance=None,
            session=session_mock,
            progress_message='progress1'
        )
        get_response_json_with_provenance_from_api_mock.assert_called_with(
            get_resolved_api_url(SOURCE_CONFIG_1.api_url, doi=DOI_1),
            params=SOURCE_CONFIG_1.params,
            headers=ANY,
            printable_headers=ANY,
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
        get_article_response_json_from_api(
            DOI_1,
            source_config=SOURCE_CONFIG_1._replace(headers=headers_config)
        )
        _args, kwargs = get_response_json_with_provenance_from_api_mock.call_args
        assert kwargs['headers'] == headers_config.mapping
        assert kwargs['printable_headers'] == headers_config.printable_mapping

    @pytest.mark.parametrize(
        'input_provenance,expected_provenance', [
            (
                None,
                {'doi': DOI_1}
            ),
            (
                {'other_key': 'other_value'},
                {'other_key': 'other_value', 'doi': DOI_1}
            )
        ])
    def test_should_add_doi_to_provenance(
        self,
        session_mock: MagicMock,
        get_response_json_with_provenance_from_api_mock: MagicMock,
        input_provenance: Optional[dict],
        expected_provenance: dict
    ):
        get_article_response_json_from_api(
            DOI_1,
            source_config=SOURCE_CONFIG_1,
            provenance=input_provenance,
            session=session_mock,
            progress_message='progress1'
        )
        _args, kwargs = get_response_json_with_provenance_from_api_mock.call_args
        assert kwargs['provenance'] == expected_provenance


class TestGetProgressMessage:
    def test_should_return_position_only_if_iterable_has_no_size(self):
        assert get_progress_message(0, iter([DOI_1])) == '1'

    def test_should_return_position_with_total_if_iterable_has_size(self):
        assert get_progress_message(0, [DOI_1]) == '1/1'


class TestIterArticleData:
    def test_should_call_fetch_article_by_doi_with_doi(
        self,
        session_mock: MagicMock,
        get_article_response_json_from_api_mock: MagicMock
    ):
        doi_iterable = iter([DOI_1])
        result = list(iter_article_data(
            doi_iterable,
            source_config=SOURCE_CONFIG_1,
            provenance=PROVENANCE_1,
            session=session_mock
        ))
        assert result == [get_article_response_json_from_api_mock.return_value]
        get_article_response_json_from_api_mock.assert_called_with(
            DOI_1,
            source_config=SOURCE_CONFIG_1,
            provenance=PROVENANCE_1,
            session=session_mock,
            progress_message=get_progress_message(0, doi_iterable)
        )


class TestFetchArticleDataFromSemanticScholarAndLoadIntoBigQuery:
    def test_should_pass_provenance_to_iter_article_data(
        self,
        iter_article_data_mock: MagicMock
    ):
        json_list = [ITEM_RESPONSE_JSON_1]
        iter_article_data_mock.return_value = json_list
        fetch_article_data_from_semantic_scholar_and_load_into_bigquery(
            CONFIG_1
        )
        iter_article_data_mock.assert_called_with(
            ANY,
            source_config=ANY,
            provenance={'imported_timestamp': MOCK_UTC_NOW_STR},
            session=ANY
        )

    def test_should_pass_iter_doi_result_to_iter_article_data(
        self,
        iter_doi_for_matrix_config_mock: MagicMock,
        iter_article_data_mock: MagicMock
    ):
        json_list = [ITEM_RESPONSE_JSON_1]
        iter_article_data_mock.return_value = json_list
        fetch_article_data_from_semantic_scholar_and_load_into_bigquery(
            CONFIG_1
        )
        iter_doi_for_matrix_config_mock.assert_called_with(
            MATRIX_CONFIG_1
        )
        doi_iterable = iter_doi_for_matrix_config_mock.return_value
        iter_article_data_mock.assert_called_with(
            doi_iterable,
            source_config=SOURCE_CONFIG_1,
            provenance=ANY,
            session=ANY
        )

    def test_should_pass_retry_session_to_iter_article_data(
        self,
        requests_retry_session_mock: MagicMock,
        session_mock: MagicMock,
        iter_article_data_mock: MagicMock
    ):
        fetch_article_data_from_semantic_scholar_and_load_into_bigquery(
            CONFIG_1
        )
        requests_retry_session_mock.assert_called()
        iter_article_data_mock.assert_called_with(
            ANY,
            source_config=SOURCE_CONFIG_1,
            provenance=ANY,
            session=session_mock
        )

    def test_should_pass_project_dataset_and_table_to_bq_load_method(
        self,
        iter_article_data_mock: MagicMock,
        load_given_json_list_data_from_tempdir_to_bq_mock: MagicMock
    ):
        json_list = [ITEM_RESPONSE_JSON_1]
        iter_article_data_mock.return_value = json_list
        fetch_article_data_from_semantic_scholar_and_load_into_bigquery(
            CONFIG_1
        )
        load_given_json_list_data_from_tempdir_to_bq_mock.assert_called()
        load_given_json_list_data_from_tempdir_to_bq_mock.assert_called_with(
            project_name=TARGET_CONFIG_1.project_name,
            dataset_name=TARGET_CONFIG_1.dataset_name,
            table_name=TARGET_CONFIG_1.table_name,
            json_list=ANY
        )
