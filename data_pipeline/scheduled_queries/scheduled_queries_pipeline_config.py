from dataclasses import dataclass
from typing import Sequence

from data_pipeline.scheduled_queries.scheduled_queries_pipeline_config_typing import (
    MultiScheduledQueryPipelineConfigDict,
    ScheduledBigQueryConfigDict,
    ScheduledQueryPipelineConfigDict
)
from data_pipeline.utils.pipeline_config import get_pipeline_config_for_env_name_and_config_parser


@dataclass(frozen=True)
class ScheduledBigQueryConfig:
    project_name: str
    sql_query: str

    @staticmethod
    def from_dict(
        bigquery_config_dict: ScheduledBigQueryConfigDict
    ) -> 'ScheduledBigQueryConfig':
        return ScheduledBigQueryConfig(
            project_name=bigquery_config_dict['projectName'],
            sql_query=bigquery_config_dict['sqlQuery']
        )


@dataclass(frozen=True)
class ScheduledQueryPipelineConfig:
    data_pipeline_id: str
    bigquery: ScheduledBigQueryConfig

    @staticmethod
    def from_dict(
        pipeline_config_dict: ScheduledQueryPipelineConfigDict
    ) -> 'ScheduledQueryPipelineConfig':
        return ScheduledQueryPipelineConfig(
            data_pipeline_id=pipeline_config_dict['dataPipelineId'],
            bigquery=ScheduledBigQueryConfig.from_dict(
                pipeline_config_dict['bigQuery']
            )
        )


@dataclass(frozen=True)
class MultiScheduledQueryPipelineConfig:
    scheduled_queries: Sequence[ScheduledQueryPipelineConfig]

    @staticmethod
    def from_dict(
        multi_pipeline_config_dict: MultiScheduledQueryPipelineConfigDict
    ) -> 'MultiScheduledQueryPipelineConfig':
        return MultiScheduledQueryPipelineConfig(
            scheduled_queries=[
                ScheduledQueryPipelineConfig.from_dict(query_config_dict)
                for query_config_dict in multi_pipeline_config_dict['scheduledQueries']
            ]
        )


class ScheduledQueriesPipelineConfigEnvironmentVariables:
    CONFIG_FILE_PATH = 'SCHEDULED_QUERIES_PIPELINE_CONFIG_FILE_PATH'


def get_multi_scheduled_queries_pipeline_config() -> MultiScheduledQueryPipelineConfig:
    return get_pipeline_config_for_env_name_and_config_parser(
        ScheduledQueriesPipelineConfigEnvironmentVariables.CONFIG_FILE_PATH,
        MultiScheduledQueryPipelineConfig.from_dict
    )
