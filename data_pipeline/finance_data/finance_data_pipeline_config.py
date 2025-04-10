from dataclasses import dataclass
from data_pipeline.finance_data.finance_data_pipeline_config_typing import (
    FinanceDataSourceConfigDict,
    FinanceDataTargetConfigDict,
    FinanceDataPipelineConfigDict,
)


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
    source: FinanceDataSourceConfig
    target: FinanceDataTargetConfig

    @staticmethod
    def from_dict(
        pipeline_config_dict: FinanceDataPipelineConfigDict
    ) -> 'FinanceDataPipelineConfig':
        return FinanceDataPipelineConfig(
            source=FinanceDataSourceConfig.from_dict(pipeline_config_dict['source']),
            target=FinanceDataTargetConfig.from_dict(pipeline_config_dict['target'])
        )
