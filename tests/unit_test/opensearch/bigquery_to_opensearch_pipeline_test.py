from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

from data_pipeline.utils.pipeline_config import BigQuerySourceConfig
from data_pipeline.opensearch.bigquery_to_opensearch_config import (
    BigQueryToOpenSearchConfig,
    BigQueryToOpenSearchSourceConfig,
    BigQueryToOpenSearchTargetConfig,
    OpenSearchTargetConfig
)
import data_pipeline.opensearch.bigquery_to_opensearch_pipeline as test_module
from data_pipeline.opensearch.bigquery_to_opensearch_pipeline import (
    fetch_documents_from_bigquery_and_load_into_opensearch,
    fetch_documents_from_bigquery_and_load_into_opensearch_from_config_list
)


BIGQUERY_TO_OPENSEARCH_CONFIG_1 = BigQueryToOpenSearchConfig(
    source=BigQueryToOpenSearchSourceConfig(
        bigquery=BigQuerySourceConfig(
            project_name='project1',
            sql_query='query 1'
        )
    ),
    target=BigQueryToOpenSearchTargetConfig(
        opensearch=OpenSearchTargetConfig(
            hostname='hostname1',
            port=9200
        )
    )
)


@pytest.fixture(name='iter_documents_from_bigquery_mock')
def _iter_documents_from_bigquery_mock() -> Iterator[MagicMock]:
    with patch.object(test_module, 'iter_documents_from_bigquery') as mock:
        yield mock


@pytest.fixture(name='load_documents_into_opensearch_mock')
def _load_documents_into_opensearch_mock() -> Iterator[MagicMock]:
    with patch.object(test_module, 'load_documents_into_opensearch') as mock:
        yield mock


@pytest.fixture(name='fetch_documents_from_bigquery_and_load_into_opensearch_mock')
def _fetch_documents_from_bigquery_and_load_into_opensearch_mock() -> Iterator[MagicMock]:
    with patch.object(
        test_module,
        'fetch_documents_from_bigquery_and_load_into_opensearch'
    ) as mock:
        yield mock


class TestFetchDocumentsFromBigQueryAndUpdateOpenSearch:
    def test_should_fetch_documents_from_bigquery_and_pass_to_opensearch(
        self,
        iter_documents_from_bigquery_mock: MagicMock,
        load_documents_into_opensearch_mock: MagicMock
    ):
        fetch_documents_from_bigquery_and_load_into_opensearch(
            BIGQUERY_TO_OPENSEARCH_CONFIG_1
        )
        iter_documents_from_bigquery_mock.assert_called_with(
            BIGQUERY_TO_OPENSEARCH_CONFIG_1.source.bigquery
        )
        load_documents_into_opensearch_mock.assert_called_with(
            iter_documents_from_bigquery_mock.return_value
        )


class TestFetchDocumentsFromBigQueryAndUpdateOpenSearchFromConfigList:
    def test_should_process_each_config_item(
        self,
        fetch_documents_from_bigquery_and_load_into_opensearch_mock: MagicMock
    ):
        fetch_documents_from_bigquery_and_load_into_opensearch_from_config_list([
            BIGQUERY_TO_OPENSEARCH_CONFIG_1
        ])
        fetch_documents_from_bigquery_and_load_into_opensearch_mock.assert_called_with(
            BIGQUERY_TO_OPENSEARCH_CONFIG_1
        )
