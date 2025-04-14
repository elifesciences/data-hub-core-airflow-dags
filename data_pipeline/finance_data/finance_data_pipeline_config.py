from dataclasses import dataclass
import logging
from typing import List
from data_pipeline.finance_data.finance_data_pipeline_config_typing import (
    FinanceDataSourceConfigDict,
    FinanceDataTargetConfigDict,
    FinanceDataPipelineConfigDict,
)

LOGGER = logging.getLogger(__name__)


@dataclass
class FinanceDataSourceConfig:
    project_name: str
    dataset: str
    table: str

    @staticmethod
    def from_dict(source_config_dict: FinanceDataSourceConfigDict) -> 'FinanceDataSourceConfig':
        return FinanceDataSourceConfig(
            project_name=source_config_dict['projectName'],
            dataset=source_config_dict['dataset'],
            table=source_config_dict['table']
        )


@dataclass
class FinanceDataTargetConfig:
    bucket: str
    object_name: str

    @staticmethod
    def from_dict(target_config_dict: FinanceDataTargetConfigDict) -> 'FinanceDataTargetConfig':
        return FinanceDataTargetConfig(
            bucket=target_config_dict['bucket'],
            object_name=target_config_dict['objectName']
        )


@dataclass
class FinanceDataPipelineConfig:
    data_pipeline_id: str
    source: FinanceDataSourceConfig
    target: FinanceDataTargetConfig

    @staticmethod
    def from_dict(
        config_dict: FinanceDataPipelineConfigDict
    ) -> 'FinanceDataPipelineConfig':
        return FinanceDataPipelineConfig(
            data_pipeline_id=config_dict['dataPipelineId'],
            source=FinanceDataSourceConfig.from_dict(config_dict['source']),
            target=FinanceDataTargetConfig.from_dict(config_dict['target'])
        )

    @staticmethod
    def parse_config_list_from_dict(
        config_dict: dict
    ) -> List['FinanceDataPipelineConfig']:
        LOGGER.debug('config_dict: %r', config_dict)
        config_dict_list = config_dict['financeDataPipeline']
        return [
            FinanceDataPipelineConfig.from_dict(config_dict)
            for config_dict in config_dict_list
        ]
