from typing import List, Sequence
from typing_extensions import NotRequired, TypedDict

from data_pipeline.utils.pipeline_config_typing import AirflowConfigDict


class KubernetesEnvConfigDict(TypedDict):
    name: str
    value: str


class KubernetesVolumeMountConfigDict(TypedDict):
    name: str
    mountPath: str
    readOnly: NotRequired[bool]


class KubernetesPipelineConfigDict(TypedDict):
    dataPipelineId: str
    airflow: NotRequired[AirflowConfigDict]
    image: str
    arguments: List[str]
    env: NotRequired[Sequence[KubernetesEnvConfigDict]]
    volumeMounts: NotRequired[Sequence[KubernetesVolumeMountConfigDict]]
    volumes: NotRequired[Sequence[dict]]
    resources: NotRequired[dict]


class KubernetesDefaultConfigDict(TypedDict):
    airflow: NotRequired[AirflowConfigDict]


class MultiKubernetesPipelineConfigDict(TypedDict):
    defaultConfig: NotRequired[KubernetesDefaultConfigDict]
    kubernetesPipelines: Sequence[KubernetesPipelineConfigDict]
