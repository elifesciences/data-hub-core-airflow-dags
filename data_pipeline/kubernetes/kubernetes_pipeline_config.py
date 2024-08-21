from dataclasses import dataclass
from typing import List, Optional, Sequence, Type

from kubernetes.client import api_client as k8s_api_client
from kubernetes.client import models as k8s_models

from data_pipeline.kubernetes.kubernetes_pipeline_config_typing import (
    KubernetesEnvConfigDict,
    KubernetesPipelineConfigDict,
    MultiKubernetesPipelineConfigDict
)
from data_pipeline.utils.pipeline_config import AirflowConfig


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
class KubernetesPipelineConfig:  # pylint: disable=too-many-instance-attributes
    data_pipeline_id: str
    image: str
    image_pull_policy: Optional[str]
    arguments: List[str]
    airflow_config: AirflowConfig
    volume_mounts: Optional[List[k8s_models.V1VolumeMount]]
    volumes: Optional[List[k8s_models.V1Volume]]
    env: Optional[List[KubernetesEnvConfigDict]]
    resources: Optional[k8s_models.V1ResourceRequirements]

    @staticmethod
    def from_dict(
        pipeline_config_dict: KubernetesPipelineConfigDict
    ) -> 'KubernetesPipelineConfig':
        return KubernetesPipelineConfig(
            data_pipeline_id=pipeline_config_dict['dataPipelineId'],
            airflow_config=AirflowConfig.from_optional_dict(pipeline_config_dict.get('airflow')),
            image=pipeline_config_dict['image'],
            image_pull_policy=pipeline_config_dict.get('imagePullPolicy'),
            arguments=pipeline_config_dict['arguments'],
            volume_mounts=[
                convert_dict_to_kubernetes_client_object(
                    config_dict,
                    k8s_models.V1VolumeMount
                )
                for config_dict in pipeline_config_dict['volumeMounts']
            ],
            volumes=[
                convert_dict_to_kubernetes_client_object(
                    config_dict,
                    k8s_models.V1Volume
                )
                for config_dict in pipeline_config_dict['volumes']
            ],
            env=[
                convert_dict_to_kubernetes_client_object(
                    config_dict,
                    k8s_models.V1EnvVar
                )
                for config_dict in pipeline_config_dict['env']
            ],
            resources=convert_dict_to_kubernetes_client_object(
                pipeline_config_dict.get('resources'),
                k8s_models.V1ResourceRequirements
            )
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
