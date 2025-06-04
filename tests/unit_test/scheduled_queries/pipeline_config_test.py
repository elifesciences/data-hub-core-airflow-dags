from datetime import date
from pathlib import Path

import yaml

from data_pipeline.utils.pipeline_config import PipelineEnvironmentVariables, StateFileConfig

from data_pipeline.scheduled_queries.pipeline_config import (
    MultiScheduledQueryPipelineConfig,
    ScheduledBigQueryConfig,
    ScheduledQueriesPipelineConfigEnvironmentVariables,
    ScheduledQueryPipelineConfig,
    ScheduledQueryPipelineInitialStateConfig,
    ScheduledQueryPipelineStateConfig,
    get_multi_scheduled_queries_pipeline_config
)
from data_pipeline.scheduled_queries.pipeline_config_typing import (
    MultiScheduledQueryPipelineConfigDict,
    ScheduledBigQueryConfigDict,
    ScheduledQueryPipelineConfigDict,
    ScheduledQueryPipelineInitialStateConfigDict,
    ScheduledQueryPipelineStateConfigDict
)
from data_pipeline.utils.pipeline_config_typing import StateFileConfigDict


BIGQUERY_CONFIG_DICT_1: ScheduledBigQueryConfigDict = {
    'projectName': 'project_1',
    'sqlQuery': 'query_1'
}


PIPELINE_CONFIG_DICT_1: ScheduledQueryPipelineConfigDict = {
    'dataPipelineId': 'data_pipeline_id',
    'bigQuery': BIGQUERY_CONFIG_DICT_1
}

STATE_FILE_CONFIG_DICT_1: StateFileConfigDict = {
    'bucketName': 'bucket_1',
    'objectName': 'object_1'
}

INITAL_STATE_CONFIG_DICT_1: ScheduledQueryPipelineInitialStateConfigDict = {
    'startDate': '2023-01-01'
}

STATE_CONFIG_DICT_1: ScheduledQueryPipelineStateConfigDict = {
    'initialState': INITAL_STATE_CONFIG_DICT_1,
    'stateFile': STATE_FILE_CONFIG_DICT_1
}

MULTI_PIPELINE_CONFIG_DICT_1: MultiScheduledQueryPipelineConfigDict = {
    'scheduledQueries': [PIPELINE_CONFIG_DICT_1]
}


class TestScheduledBigQueryConfig:
    def test_should_parse_project_name_and_sql_query(self):
        config = ScheduledBigQueryConfig.from_dict(BIGQUERY_CONFIG_DICT_1)
        assert config.project_name == BIGQUERY_CONFIG_DICT_1['projectName']
        assert config.sql_query == BIGQUERY_CONFIG_DICT_1['sqlQuery']


class TestScheduledQueryPipelineInitialStateConfig:
    def test_should_parse_start_date(self):
        config = ScheduledQueryPipelineInitialStateConfig.from_dict(INITAL_STATE_CONFIG_DICT_1)
        assert config.start_date == date.fromisoformat(
            INITAL_STATE_CONFIG_DICT_1['startDate']
        )


class TestScheduledQueryPipelineStateConfig:
    def test_should_parse_initial_state(self):
        config = ScheduledQueryPipelineStateConfig.from_dict(STATE_CONFIG_DICT_1)
        assert config.initial_state == ScheduledQueryPipelineInitialStateConfig.from_dict(
            STATE_CONFIG_DICT_1['initialState']
        )

    def test_should_parse_state_file(self):
        config = ScheduledQueryPipelineStateConfig.from_dict(STATE_CONFIG_DICT_1)
        assert config.state_file == StateFileConfig.from_dict(
            STATE_FILE_CONFIG_DICT_1
        )


class TestScheduledQueryConfig:
    def test_should_parse_bigquery_config(self):
        config = ScheduledQueryPipelineConfig.from_dict(PIPELINE_CONFIG_DICT_1)
        assert config.data_pipeline_id == PIPELINE_CONFIG_DICT_1['dataPipelineId']
        assert config.bigquery == ScheduledBigQueryConfig.from_dict(
            PIPELINE_CONFIG_DICT_1['bigQuery']
        )

    def test_should_parse_state_if_exists_in_config(self):
        config_dict = {
            **PIPELINE_CONFIG_DICT_1,
            'state': STATE_CONFIG_DICT_1
        }
        config = ScheduledQueryPipelineConfig.from_dict(config_dict)
        assert config.state == ScheduledQueryPipelineStateConfig.from_dict(
            STATE_CONFIG_DICT_1
        )


class TestMultiScheduledQueryConfig:
    def test_should_parse_scheduled_queries_list(self):
        config = MultiScheduledQueryPipelineConfig.from_dict({
            'scheduledQueries': [PIPELINE_CONFIG_DICT_1]
        })
        assert config.scheduled_queries == [
            ScheduledQueryPipelineConfig.from_dict(PIPELINE_CONFIG_DICT_1)
        ]


class TestGetMultiScheduledQueriesPipelineConfig:
    def test_should_parse_config_from_file(
        self,
        mock_env: dict,
        tmp_path: Path
    ):
        config_file_path = tmp_path / 'config.yaml'
        config_file_path.write_text(
            yaml.safe_dump(MULTI_PIPELINE_CONFIG_DICT_1),
            encoding='utf-8'
        )
        mock_env[
            ScheduledQueriesPipelineConfigEnvironmentVariables.CONFIG_FILE_PATH
        ] = str(config_file_path)
        config = get_multi_scheduled_queries_pipeline_config()
        assert config == MultiScheduledQueryPipelineConfig.from_dict(
            MULTI_PIPELINE_CONFIG_DICT_1
        )

    def test_should_resolved_env_placeholder(
        self,
        mock_env: dict,
        tmp_path: Path
    ):
        sql_query_with_placeholder = 'ENV: {ENV}'
        sql_query_with_resolved_placeholder = 'ENV: test'
        multi_config_dict_with_placeholder: MultiScheduledQueryPipelineConfigDict = {
            'scheduledQueries': [{
                **PIPELINE_CONFIG_DICT_1,
                'bigQuery': {
                    **BIGQUERY_CONFIG_DICT_1,
                    'sqlQuery': sql_query_with_placeholder
                }
            }]
        }
        config_file_path = tmp_path / 'config.yaml'
        config_file_path.write_text(
            yaml.safe_dump(multi_config_dict_with_placeholder),
            encoding='utf-8'
        )
        mock_env[
            ScheduledQueriesPipelineConfigEnvironmentVariables.CONFIG_FILE_PATH
        ] = str(config_file_path)
        mock_env[
            PipelineEnvironmentVariables.DEPLOYMENT_ENV
        ] = 'test'
        config = get_multi_scheduled_queries_pipeline_config()
        assert (
            config.scheduled_queries[0].bigquery.sql_query
            == sql_query_with_resolved_placeholder
        )
