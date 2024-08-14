from dataclasses import dataclass
from typing import Sequence

from data_pipeline.kubernetes.kubernetes_pipeline_config_typing import (
    KubernetesPipelineConfigDict,
    MultiKubernetesPipelineConfigDict
)


@dataclass(frozen=True)
class KubernetesPipelineConfig:
    data_pipeline_id: str
    image: str
    arguments: str

    @staticmethod
    def from_dict(
        pipeline_config_dict: KubernetesPipelineConfigDict
    ) -> 'KubernetesPipelineConfig':
        return KubernetesPipelineConfig(
            data_pipeline_id=pipeline_config_dict['dataPipelineId'],
            image=pipeline_config_dict['image'],
            arguments=' '.join(pipeline_config_dict['arguments'])
        )


@dataclass(frozen=True)
class MultiKubernetesPipelineConfig:
    kubernetes_pipelines: Sequence[KubernetesPipelineConfig]

    @staticmethod
    def from_dict(
        multi_pipeline_config_dict: MultiKubernetesPipelineConfigDict
    ) -> 'MultiKubernetesPipelineConfig':
        return MultiKubernetesPipelineConfig(
            kubernetes_pipelines=[
                KubernetesPipelineConfig.from_dict(pipeline_config_dict)
                for pipeline_config_dict in multi_pipeline_config_dict['kubernetesPipelines']
            ]
        )
