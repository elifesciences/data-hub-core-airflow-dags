
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


@pytest.fixture(name='process_processed_scheduled_query_mock', autouse=True)
def _process_processed_scheduled_query_mock():
    with patch.object(pipeline, 'process_processed_scheduled_query') as mock:
        yield mock


class TestProcessProcessedQueries:
    def test_should_call_process_processed_scheduled_query(
        self,
        process_processed_scheduled_query_mock: MagicMock
    ):
        pipeline.process_processed_scheduled_queries(
            MultiScheduledQueryPipelineConfig(
                scheduled_queries=[PIPELINE_CONFIG_1]
            )
        )
        process_processed_scheduled_query_mock.assert_called_with(PIPELINE_CONFIG_1)
