from datetime import datetime
from unittest.mock import ANY, MagicMock, patch

import pytest

from data_pipeline.utils.pipeline_config import (
    BigQuerySourceConfig,
    BigQueryTargetConfig
)
from data_pipeline.semantic_scholar.semantic_scholar_config import (
    SemanticScholarConfig,
    SemanticScholarMatrixConfig,
    SemanticScholarMatrixVariableConfig,
    SemanticScholarMatrixVariableSourceConfig,
    SemanticScholarSourceConfig
)
from data_pipeline.semantic_scholar import (
    semantic_scholar_recommendation_pipeline as semantic_scholar_recommendation_pipeline_module
)
from data_pipeline.semantic_scholar.semantic_scholar_recommendation_pipeline import (
    fetch_article_data_from_semantic_scholar_recommendation_and_load_into_bigquery
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


MATRIX_VARIABLE_CONFIG_1 = SemanticScholarMatrixVariableConfig(
    include=SemanticScholarMatrixVariableSourceConfig(
        bigquery=BIGQUERY_SOURCE_CONFIG_1
    )
)


MATRIX_VARIABLE_WITH_EXCLUDE_CONFIG_1 = SemanticScholarMatrixVariableConfig(
    include=SemanticScholarMatrixVariableSourceConfig(
        bigquery=BIGQUERY_SOURCE_CONFIG_1
    ),
    exclude=SemanticScholarMatrixVariableSourceConfig(
        bigquery=BIGQUERY_SOURCE_CONFIG_2
    )
)


MATRIX_CONFIG_1 = SemanticScholarMatrixConfig(
    variables={
        'list': MATRIX_VARIABLE_CONFIG_1
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


@pytest.fixture(name='iter_list_for_matrix_config_mock')
def _iter_list_for_matrix_config_mock():
    with patch.object(
        semantic_scholar_recommendation_pipeline_module,
        'iter_list_for_matrix_config'
    ) as mock:
        yield mock


@pytest.fixture(name='iter_recommendation_data_mock')
def _iter_recommendation_data_mock():
    with patch.object(
        semantic_scholar_recommendation_pipeline_module,
        'iter_recommendation_data'
    ) as mock:
        yield mock


class TestFetchArticleDataFromSemanticScholarRecommendationAndLoadIntoBigQuery:
    def test_should_pass_provenance_to_iter_article_data(
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

    def test_should_pass_iter_doi_result_to_iter_article_data(
        self,
        iter_list_for_matrix_config_mock: MagicMock,
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
        doi_iterable = iter_list_for_matrix_config_mock.return_value
        iter_recommendation_data_mock.assert_called_with(
            doi_iterable,
            source_config=SOURCE_CONFIG_1,
            provenance=ANY,
            session=ANY
        )

    def test_should_pass_retry_session_to_iter_article_data(
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
