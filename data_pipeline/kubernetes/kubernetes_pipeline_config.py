from dataclasses import dataclass
from typing import List, Optional, Sequence, Type

from kubernetes.client import api_client as k8s_api_client
from kubernetes.client import models as k8s_models

from data_pipeline.kubernetes.kubernetes_pipeline_config_typing import (
    KubernetesEnvConfigDict,
    KubernetesPipelineConfigDict,
    MultiKubernetesPipelineConfigDict
)


def convert_dict_to_kubernetes_client_object(
    config_dict,
    kubernetes_model_class: Type
):
    api_client = k8s_api_client.ApiClient()
    return api_client._ApiClient__deserialize_model(  # pylint: disable=protected-access
        config_dict,
        kubernetes_model_class
    )


@dataclass(frozen=True)
class KubernetesPipelineConfig:
    data_pipeline_id: str
    image: str
    arguments: List[str]
    volume_mounts: Optional[List[k8s_models.v1_volume_mount.V1VolumeMount]]
    volumes: Optional[List[dict]]
    env: Optional[List[KubernetesEnvConfigDict]]

    @staticmethod
    def from_dict(
        pipeline_config_dict: KubernetesPipelineConfigDict
    ) -> 'KubernetesPipelineConfig':
        return KubernetesPipelineConfig(
            data_pipeline_id=pipeline_config_dict['dataPipelineId'],
            image=pipeline_config_dict['image'],
            arguments=pipeline_config_dict['arguments'],
            volume_mounts=[
                convert_dict_to_kubernetes_client_object(
                    config_dict,
                    k8s_models.v1_volume_mount.V1VolumeMount
                )
                for config_dict in pipeline_config_dict['volumeMounts']
            ],
            volumes=[
                convert_dict_to_kubernetes_client_object(
                    config_dict,
                    k8s_models.v1_volume.V1Volume
                )
                for config_dict in pipeline_config_dict['volumes']
            ],
            env=[
                convert_dict_to_kubernetes_client_object(
                    config_dict,
                    k8s_models.v1_env_var.V1EnvVar
                )
                for config_dict in pipeline_config_dict['env']
            ]
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
