from dataclasses import dataclass
import logging
import os
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Type, TypeVar, cast

from kubernetes.client import api_client as k8s_api_client
from kubernetes.client import models as k8s_models

from data_pipeline.kubernetes.kubernetes_pipeline_config_typing import (
    KubernetesDefaultConfigDict,
    KubernetesEnvConfigDict,
    KubernetesPipelineConfigDict,
    KubernetesPipelineFileConfigDict,
    MultiKubernetesPipelineConfigDict
)
from data_pipeline.utils.pipeline_config import (
    AirflowConfig,
    get_deployment_env,
    update_deployment_env_placeholder
)
from data_pipeline.utils.pipeline_file_io import get_yaml_file_as_dict


LOGGER = logging.getLogger(__name__)


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
    def iter_pipeline_config_from_config_files(
        config_files: Sequence[str],
        base_path: str
    ) -> Iterable['KubernetesPipelineConfig']:
        for import_file in config_files:
            full_import_file = os.path.join(base_path, import_file)
            LOGGER.info('Importing from: %r', full_import_file)
            pipeline_config_file_dict = cast(
                KubernetesPipelineFileConfigDict,
                get_yaml_file_as_dict(full_import_file)
            )
            yield from KubernetesPipelineFileConfig.from_dict(
                pipeline_config_file_dict,
                base_path=base_path
            ).kubernetes_pipelines

    @staticmethod
    def from_dict(
        pipeline_config_file_dict: KubernetesPipelineFileConfigDict,
        base_path: str = '.'
    ) -> 'KubernetesPipelineFileConfig':
        if 'importFromFiles' in pipeline_config_file_dict:
            return KubernetesPipelineFileConfig(
                kubernetes_pipelines=list(
                    KubernetesPipelineFileConfig.iter_pipeline_config_from_config_files(
                        pipeline_config_file_dict['importFromFiles'],  # type: ignore
                        base_path=base_path
                    )
                )
            )
        return KubernetesPipelineFileConfig(
            kubernetes_pipelines=MultiKubernetesPipelineConfig.from_dict(
                pipeline_config_file_dict  # type: ignore
            ).kubernetes_pipelines
        )


class KubernetesPipelineConfigEnvironmentVariables:
    CONFIG_FILE_PATH = 'KUBERNETES_PIPELINE_CONFIG_FILE_PATH'


def get_multi_kubernetes_pipeline_config() -> KubernetesPipelineFileConfig:
    deployment_env = get_deployment_env()
    LOGGER.info('deployment_env: %s', deployment_env)
    config_file_path = os.environ[KubernetesPipelineConfigEnvironmentVariables.CONFIG_FILE_PATH]
    pipeline_config_dict = update_deployment_env_placeholder(
        get_yaml_file_as_dict(config_file_path),
        deployment_env=deployment_env
    )
    LOGGER.info('pipeline_config_dict: %s', pipeline_config_dict)
    pipeline_config = KubernetesPipelineFileConfig.from_dict(
        pipeline_config_dict,
        base_path=os.path.dirname(config_file_path)
    )
    LOGGER.info('pipeline_config: %s', pipeline_config)
    return pipeline_config
