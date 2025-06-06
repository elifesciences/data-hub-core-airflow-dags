
import dataclasses
from datetime import date
from unittest.mock import ANY, patch, MagicMock

import pytest

from data_pipeline.utils.pipeline_config import StateFileConfig

from data_pipeline.scheduled_queries.pipeline_config import (
    MultiScheduledQueryPipelineConfig,
    ScheduledBigQueryConfig,
    ScheduledQueryPipelineConfig,
    ScheduledQueryPipelineInitialStateConfig,
    ScheduledQueryPipelineStateConfig
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

STATE_FILE_CONFIG_1 = StateFileConfig(
    bucket_name='bucket_name_1',
    object_name='object_name_1'
)

STATE_CONFIG_1 = ScheduledQueryPipelineStateConfig(
    initial_state=ScheduledQueryPipelineInitialStateConfig(
        start_date=date.fromisoformat('2025-05-26')
    ),
    state_file=STATE_FILE_CONFIG_1
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


@pytest.fixture(name='update_state_file_mock')
def _update_state_file_mock():
    with patch.object(pipeline, 'update_state_file') as mock:
        yield mock


@pytest.fixture(name='upload_s3_object_mock', autouse=True)
def _upload_s3_object_mock():
    with patch.object(pipeline, 'upload_s3_object') as mock:
        yield mock


@pytest.fixture(name='download_s3_object_as_string_or_file_not_found_error_mock', autouse=True)
def _download_s3_object_as_string_or_file_not_found_error_mock():
    with patch.object(
        pipeline,
        'download_s3_object_as_string_or_file_not_found_error'
    ) as mock:
        mock.return_value = '2001-01-01'
        yield mock


@pytest.fixture(name='load_state_or_default_from_s3_for_config_mock')
def _load_state_or_default_from_s3_for_config_mock():
    with patch.object(
        pipeline,
        'load_state_or_default_from_s3_for_config'
    ) as mock:
        yield mock


class TestReplaceStartDateInSqlQuery:
    def test_should_replace_start_date_in_sql_query(self):
        sql_query = 'SELECT * FROM table WHERE date = "{start_date}"'
        expected_query = 'SELECT * FROM table WHERE date = "20250525"'
        result_query = pipeline.replace_start_date_in_sql_query(
            sql_query, start_date=date.fromisoformat('2025-05-25')
        )
        assert result_query == expected_query


class TestSaveStateToS3ForConfig:
    def test_should_pass_bucket_and_object_to_upload_s3_object(
        self,
        upload_s3_object_mock: MagicMock
    ):
        pipeline.update_state_file(
            state_file=STATE_FILE_CONFIG_1,
            current_date=date.fromisoformat('2025-05-25')
        )
        upload_s3_object_mock.assert_called_with(
            bucket=STATE_FILE_CONFIG_1.bucket_name,
            object_key=STATE_FILE_CONFIG_1.object_name,
            data_object=ANY
        )

    def test_should_passed_in_start_date_to_upload_s3_object(
        self,
        upload_s3_object_mock: MagicMock
    ):
        pipeline.update_state_file(
            state_file=STATE_FILE_CONFIG_1,
            current_date=date.fromisoformat('2025-05-25')
        )
        upload_s3_object_mock.assert_called_with(
            bucket=ANY,
            object_key=ANY,
            data_object='2025-05-25'
        )


class TestLoadStateOrDefaulthFromS3ForConfig:
    def test_should_call_download_s3_object_as_string(
        self,
        download_s3_object_as_string_or_file_not_found_error_mock: MagicMock
    ):
        download_s3_object_as_string_or_file_not_found_error_mock.return_value = '2025-05-25'
        result = pipeline.load_state_or_default_from_s3_for_config(
            STATE_CONFIG_1
        )
        download_s3_object_as_string_or_file_not_found_error_mock.assert_called_with(
            bucket=STATE_CONFIG_1.state_file.bucket_name,
            object_key=STATE_CONFIG_1.state_file.object_name
        )
        assert result == date.fromisoformat('2025-05-25')

    def test_should_return_initial_state_if_file_does_not_exist(
        self,
        download_s3_object_as_string_or_file_not_found_error_mock: MagicMock
    ):
        download_s3_object_as_string_or_file_not_found_error_mock.side_effect = (
            FileNotFoundError()
        )
        result = pipeline.load_state_or_default_from_s3_for_config(
            STATE_CONFIG_1
        )
        download_s3_object_as_string_or_file_not_found_error_mock.assert_called_with(
            bucket=STATE_CONFIG_1.state_file.bucket_name,
            object_key=STATE_CONFIG_1.state_file.object_name
        )
        assert result == STATE_CONFIG_1.initial_state.start_date


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

    def test_should_replace_sql_query_placeholder_if_state_is_configured(
        self,
        bq_client_mock: MagicMock,
        load_state_or_default_from_s3_for_config_mock: MagicMock
    ):
        load_state_or_default_from_s3_for_config_mock.return_value = (
            date.fromisoformat('2025-05-27')
        )
        pipeline.process_scheduled_query(
            dataclasses.replace(
                PIPELINE_CONFIG_1,
                bigquery=dataclasses.replace(
                    PIPELINE_CONFIG_1.bigquery,
                    sql_query='SELECT * FROM table WHERE date = "{start_date}"'
                ),
                state=STATE_CONFIG_1
            )
        )
        load_state_or_default_from_s3_for_config_mock.assert_called_with(
            STATE_CONFIG_1
        )
        bq_client_mock.query.assert_called_with(
            'SELECT * FROM table WHERE date = "20250527"'
        )

    def test_should_update_state_file_with_current_date(
        self,
        update_state_file_mock: MagicMock
    ):
        pipeline.process_scheduled_query(
            dataclasses.replace(
                PIPELINE_CONFIG_1,
                state=STATE_CONFIG_1
            )
        )
        update_state_file_mock.assert_called_with(
            state_file=STATE_CONFIG_1.state_file,
            current_date=ANY
        )


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
