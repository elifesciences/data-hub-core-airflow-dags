from unittest.mock import MagicMock, patch

import pytest

from data_pipeline.utils.pipeline_config import BigQuerySourceConfig
from data_pipeline.utils.pipeline_utils import (
    fetch_single_column_value_list_for_bigquery_source_config
)
from data_pipeline.utils import (
    pipeline_utils as pipeline_utils_module
)


BIGQUERY_SOURCE_CONFIG_1 = BigQuerySourceConfig(
    project_name='project1',
    sql_query='query1'
)


@pytest.fixture(name='get_single_column_value_list_from_bq_query_mock', autouse=True)
def _get_single_column_value_list_from_bq_query_mock():
    with patch.object(
        pipeline_utils_module,
        'get_single_column_value_list_from_bq_query'
    ) as mock:
        yield mock


class TestFetchSingleColumnValueListForBigQuerySourceConfig:
    def test_should_call_get_single_column_value_list_from_bq_query(
        self,
        get_single_column_value_list_from_bq_query_mock: MagicMock
    ):
        fetch_single_column_value_list_for_bigquery_source_config(BIGQUERY_SOURCE_CONFIG_1)
        get_single_column_value_list_from_bq_query_mock.assert_called_with(
            project_name=BIGQUERY_SOURCE_CONFIG_1.project_name,
            query=BIGQUERY_SOURCE_CONFIG_1.sql_query
        )

    def test_should_return_doi_list_from_bq_query(
        self,
        get_single_column_value_list_from_bq_query_mock: MagicMock
    ):
        get_single_column_value_list_from_bq_query_mock.return_value = [
            'value1', 'value2'
        ]
        actual_doi_list = fetch_single_column_value_list_for_bigquery_source_config(
            BIGQUERY_SOURCE_CONFIG_1
        )
        assert actual_doi_list == get_single_column_value_list_from_bq_query_mock.return_value
