from data_pipeline.scheduled_queries.scheduled_queries_pipeline_config import (
    MultiScheduledQueryPipelineConfig,
    ScheduledBigQueryConfig,
    ScheduledQueryPipelineConfig
)
from data_pipeline.scheduled_queries.scheduled_queries_pipeline_config_typing import (
    ScheduledBigQueryConfigDict,
    ScheduledQueryPipelineConfigDict
)


BIGQUERY_CONFIG_DICT_1: ScheduledBigQueryConfigDict = {
    'projectName': 'project_1',
    'sqlQuery': 'query_1'
}


PIPELINE_CONFIG_DICT_1: ScheduledQueryPipelineConfigDict = {
    'dataPipelineId': 'data_pipeline_id',
    'bigQuery': BIGQUERY_CONFIG_DICT_1
}


class TestScheduledBigQueryConfig:
    def test_should_parse_project_name_and_sql_query(self):
        config = ScheduledBigQueryConfig.from_dict(BIGQUERY_CONFIG_DICT_1)
        assert config.project_name == BIGQUERY_CONFIG_DICT_1['projectName']
        assert config.sql_query == BIGQUERY_CONFIG_DICT_1['sqlQuery']


class TestScheduledQueryConfig:
    def test_should_parse_bigquery_config(self):
        config = ScheduledQueryPipelineConfig.from_dict(PIPELINE_CONFIG_DICT_1)
        assert config.data_pipeline_id == PIPELINE_CONFIG_DICT_1['dataPipelineId']
        assert config.bigquery == ScheduledBigQueryConfig.from_dict(
            PIPELINE_CONFIG_DICT_1['bigQuery']
        )


class TestMultiScheduledQueryConfig:
    def test_should_parse_scheduled_queries_list(self):
        config = MultiScheduledQueryPipelineConfig.from_dict({
            'scheduledQueries': [PIPELINE_CONFIG_DICT_1]
        })
        assert config.scheduled_queries == [
            ScheduledQueryPipelineConfig.from_dict(PIPELINE_CONFIG_DICT_1)
        ]
