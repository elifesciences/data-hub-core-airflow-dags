from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

from data_pipeline.utils.pipeline_config import BigQuerySourceConfig
from data_pipeline.opensearch.bigquery_to_opensearch_config import (
    BigQueryToOpenSearchConfig,
    BigQueryToOpenSearchSourceConfig
)
import data_pipeline.opensearch.bigquery_to_opensearch_pipeline as test_module
from data_pipeline.opensearch.bigquery_to_opensearch_pipeline import (
    fetch_documents_from_bigquery_and_update_opensearch_from_config_list
)


BIGQUERY_TO_OPENSEARCH_CONFIG_1 = BigQueryToOpenSearchConfig(
    source=BigQueryToOpenSearchSourceConfig(
        bigquery=BigQuerySourceConfig(
            project_name='project1',
            sql_query='query 1'
        )
    )
)


@pytest.fixture(name='fetch_documents_from_bigquery_and_update_opensearch_mock')
def _fetch_documents_from_bigquery_and_update_opensearch_mock() -> Iterator[MagicMock]:
    with patch.object(test_module, 'fetch_documents_from_bigquery_and_update_opensearch') as mock:
        yield mock


class TestFetchDocumentsFromBigQueryAndUpdateOpenSearchFromConfigList:
    def test_should_process_each_config_item(
        self,
        fetch_documents_from_bigquery_and_update_opensearch_mock: MagicMock
    ):
        fetch_documents_from_bigquery_and_update_opensearch_from_config_list([
            BIGQUERY_TO_OPENSEARCH_CONFIG_1
        ])
        fetch_documents_from_bigquery_and_update_opensearch_mock.assert_called_with(
            BIGQUERY_TO_OPENSEARCH_CONFIG_1
        )
