from dataclasses import dataclass
from typing import Any, List, Mapping, Optional, Sequence, Type, TypeVar

from kubernetes.client import api_client as k8s_api_client
from kubernetes.client import models as k8s_models

from data_pipeline.kubernetes.kubernetes_pipeline_config_typing import (
    KubernetesDefaultConfigDict,
    KubernetesEnvConfigDict,
    KubernetesPipelineConfigDict,
    KubernetesPipelineFileConfigDict,
    MultiKubernetesPipelineConfigDict
)
from data_pipeline.utils.pipeline_config import AirflowConfig

MappingT = TypeVar("MappingT", bound=Mapping[str, Any])


def convert_dict_to_kubernetes_client_object(
    config_dict,
    kubernetes_model_class: Type
):
    api_client = k8s_api_client.ApiClient()
    return api_client._ApiClient__deserialize_model(  # pylint: disable=protected-access
        config_dict,
        kubernetes_model_class
    )


def get_config_dict_for_config_dict_list(
    config_dict_list: Sequence[MappingT],
    unique_key: str
) -> Mapping[str, MappingT]:
    return {
        config_dict[unique_key]: config_dict
        for config_dict in config_dict_list
        if unique_key in config_dict
    }


def get_merged_config_dict_list(
    config: Optional[Sequence[MappingT]],
    default_config: Optional[Sequence[MappingT]],
    unique_key: str
) -> List[MappingT]:
    return list({
        **get_config_dict_for_config_dict_list(default_config or [], unique_key),
        **get_config_dict_for_config_dict_list(config or [], unique_key)
    }.values())


@dataclass(frozen=True)
class KubernetesPipelineConfig:  # pylint: disable=too-many-instance-attributes
    data_pipeline_id: str
    image: str
    arguments: List[str]
    airflow_config: AirflowConfig
    volume_mounts: Optional[List[k8s_models.V1VolumeMount]]
    volumes: Optional[List[k8s_models.V1Volume]]
    env: Optional[List[KubernetesEnvConfigDict]]
    resources: Optional[k8s_models.V1ResourceRequirements]

    @staticmethod
    def from_dict(
        pipeline_config_dict: KubernetesPipelineConfigDict,
        default_config_dict: Optional[KubernetesDefaultConfigDict] = None
    ) -> 'KubernetesPipelineConfig':
        default_config_dict = default_config_dict or {}
        return KubernetesPipelineConfig(
            data_pipeline_id=pipeline_config_dict['dataPipelineId'],
            airflow_config=AirflowConfig.from_optional_dict(
                airflow_config_dict=pipeline_config_dict.get('airflow'),
                default_airflow_config=AirflowConfig.from_optional_dict(
                    default_config_dict.get('airflow')
                )
            ),
            image=pipeline_config_dict['image'],
            arguments=pipeline_config_dict['arguments'],
            volume_mounts=[
                convert_dict_to_kubernetes_client_object(
                    config_dict,
                    k8s_models.V1VolumeMount
                )
                for config_dict in get_merged_config_dict_list(
                    config=pipeline_config_dict.get('volumeMounts', []),
                    default_config=default_config_dict.get('volumeMounts', []),
                    unique_key='mountPath'
                )
            ],
            volumes=[
                convert_dict_to_kubernetes_client_object(
                    config_dict,
                    k8s_models.V1Volume
                )
                for config_dict in get_merged_config_dict_list(
                    config=pipeline_config_dict.get('volumes', []),
                    default_config=default_config_dict.get('volumes', []),
                    unique_key='name'
                )
            ],
            env=[
                convert_dict_to_kubernetes_client_object(
                    config_dict,
                    k8s_models.V1EnvVar
                )
                for config_dict in get_merged_config_dict_list(
                    config=pipeline_config_dict.get('env'),
                    default_config=default_config_dict.get('env'),
                    unique_key='name'
                )
            ],
            resources=convert_dict_to_kubernetes_client_object(
                {
                    **(default_config_dict.get('resources') or {}),  # type: ignore
                    **(pipeline_config_dict.get('resources') or {})  # type: ignore
                },
                k8s_models.V1ResourceRequirements
            )
        )


def get_merged_kubernetes_config_dict(
    pipeline_config_dict: KubernetesPipelineConfigDict,
    default_config_dict: Optional[KubernetesDefaultConfigDict]
) -> KubernetesPipelineConfigDict:
    if not default_config_dict:
        return pipeline_config_dict
    return {
        **default_config_dict,
        **pipeline_config_dict
    }


@dataclass(frozen=True)
class MultiKubernetesPipelineConfig:
    kubernetes_pipelines: Sequence[KubernetesPipelineConfig]

    @staticmethod
    def from_dict(
        multi_pipeline_config_dict: MultiKubernetesPipelineConfigDict
    ) -> 'MultiKubernetesPipelineConfig':
        default_config_dict: Optional[KubernetesDefaultConfigDict] = (
            multi_pipeline_config_dict.get('defaultConfig')
        )
        return MultiKubernetesPipelineConfig(
            kubernetes_pipelines=[
                KubernetesPipelineConfig.from_dict(
                    pipeline_config_dict=pipeline_config_dict,
                    default_config_dict=default_config_dict
                )
                for pipeline_config_dict in multi_pipeline_config_dict['kubernetesPipelines']
            ]
        )


@dataclass(frozen=True)
class KubernetesPipelineFileConfig:
    kubernetes_pipelines: Sequence[KubernetesPipelineConfig]

    @staticmethod
    def from_dict(
        pipeline_config_file_dict: KubernetesPipelineFileConfigDict
    ) -> 'KubernetesPipelineFileConfig':
        return KubernetesPipelineFileConfig(
            kubernetes_pipelines=MultiKubernetesPipelineConfig.from_dict(
                pipeline_config_file_dict  # type: ignore
            ).kubernetes_pipelines
        )
