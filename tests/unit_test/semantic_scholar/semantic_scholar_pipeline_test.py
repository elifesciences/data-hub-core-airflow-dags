from unittest.mock import ANY, MagicMock, patch

import pytest

from data_pipeline.utils.pipeline_config import BigQueryTargetConfig

from data_pipeline.semantic_scholar.semantic_scholar_config import (
    BigQuerySourceConfig,
    SemanticScholarConfig,
    SemanticScholarMatrixConfig,
    SemanticScholarMatrixVariableConfig,
    SemanticScholarMatrixVariableSourceConfig,
    SemanticScholarSourceConfig
)

from data_pipeline.semantic_scholar import (
    semantic_scholar_pipeline as semantic_scholar_pipeline_module
)
from data_pipeline.semantic_scholar.semantic_scholar_pipeline import (
    fetch_article_by_doi,
    fetch_article_data_from_semantic_scholar_and_load_into_bigquery,
    iter_article_data
)


DOI_1 = 'doi1'
DOI_2 = 'doi2'

ITEM_RESPONSE_JSON_1 = {
    'externalIds': {
        'DOI': DOI_1
    }
}


MATRIX_CONFIG_1 = SemanticScholarMatrixConfig(
    variables={
        'doi': SemanticScholarMatrixVariableConfig(
            include=SemanticScholarMatrixVariableSourceConfig(
                bigquery=BigQuerySourceConfig(
                    project_name='project1',
                    sql_query='query1'
                )
            )
        )
    }
)


SOURCE_CONFIG_1 = SemanticScholarSourceConfig(
    api_url='api1'
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


@pytest.fixture(name='load_given_json_list_data_from_tempdir_to_bq_mock', autouse=True)
def _load_given_json_list_data_from_tempdir_to_bq_mock():
    with patch.object(
        semantic_scholar_pipeline_module,
        'load_given_json_list_data_from_tempdir_to_bq'
    ) as mock:
        yield mock


@pytest.fixture(name='iter_article_data_mock')
def _iter_article_data_mock():
    with patch.object(semantic_scholar_pipeline_module, 'iter_article_data') as mock:
        yield mock


class TestIterArticleData:
    def test_should_call_fetch_article_by_doi_with_doi(
        self
    ):
        result = list(iter_article_data([DOI_1, DOI_2]))
        assert result == [fetch_article_by_doi(DOI_1), fetch_article_by_doi(DOI_2)]


class TestFetchArticleDataFromSemanticScholarAndLoadIntoBigQuery:
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
