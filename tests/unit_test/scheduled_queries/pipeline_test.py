
from datetime import date
from unittest.mock import patch, MagicMock

import pytest

from data_pipeline.scheduled_queries.pipeline_config import (
    MultiScheduledQueryPipelineConfig,
    ScheduledBigQueryConfig,
    ScheduledQueryPipelineConfig
)

from data_pipeline.scheduled_queries import pipeline


BIGQUERY_CONFIG_1 = ScheduledBigQueryConfig(
    project_name='project_name_1',
    sql_query='sql_query_1'
)

PIPELINE_CONFIG_1 = ScheduledQueryPipelineConfig(
    data_pipeline_id='data_pipeline_1',
    bigquery=BIGQUERY_CONFIG_1
)


@pytest.fixture(name='get_bq_client_mock', autouse=True)
def _get_bq_client_mock():
    with patch.object(pipeline, 'get_bq_client') as mock:
        yield mock


@pytest.fixture(name='bq_client_mock')
def _bq_client_mock(get_bq_client_mock: MagicMock):
    return get_bq_client_mock.return_value


@pytest.fixture(name='query_job_mock')
def _query_job_mock(bq_client_mock: MagicMock):
    return bq_client_mock.query.return_value


@pytest.fixture(name='process_scheduled_query_mock')
def _process_scheduled_query_mock():
    with patch.object(pipeline, 'process_scheduled_query') as mock:
        yield mock


class TestReplaceStartDateInSqlQuery:
    def test_should_not_replace_if_no_placeholder(self):
        sql_query = 'SELECT * FROM table'
        result_query = pipeline.replace_start_date_in_sql_query(sql_query)
        assert result_query == sql_query

    def test_should_replace_start_date_in_sql_query(self):
        sql_query = 'SELECT * FROM table WHERE date = "{start_date}"'
        expected_query = 'SELECT * FROM table WHERE date = "20250525"'
        result_query = pipeline.replace_start_date_in_sql_query(
            sql_query, start_date=date.fromisoformat('2025-05-25')
        )
        assert result_query == expected_query


class TestProcessScheduledQuery:
    def test_should_call_get_bq_client(
        self,
        get_bq_client_mock: MagicMock
    ):
        pipeline.process_scheduled_query(PIPELINE_CONFIG_1)
        get_bq_client_mock.assert_called_with(
            project=PIPELINE_CONFIG_1.bigquery.project_name
        )

    def test_should_call_client_query(
        self,
        bq_client_mock: MagicMock
    ):
        pipeline.process_scheduled_query(PIPELINE_CONFIG_1)
        bq_client_mock.query.assert_called_with(PIPELINE_CONFIG_1.bigquery.sql_query)

    def test_should_call_query_job_result_to_execute(
        self,
        query_job_mock: MagicMock
    ):
        pipeline.process_scheduled_query(PIPELINE_CONFIG_1)
        query_job_mock.result.assert_called()


class TestProcessScheduledQueries:
    def test_should_call_process_processed_scheduled_query(
        self,
        process_scheduled_query_mock: MagicMock
    ):
        pipeline.process_scheduled_queries(
            MultiScheduledQueryPipelineConfig(
                scheduled_queries=[PIPELINE_CONFIG_1]
            )
        )
        process_scheduled_query_mock.assert_called_with(PIPELINE_CONFIG_1)
