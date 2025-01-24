from typing import List, Sequence, Union
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
    imagePullPolicy: NotRequired[str]
    arguments: List[str]
    env: NotRequired[Sequence[KubernetesEnvConfigDict]]
    volumeMounts: NotRequired[Sequence[KubernetesVolumeMountConfigDict]]
    volumes: NotRequired[Sequence[dict]]
    resources: NotRequired[dict]


class KubernetesDefaultConfigDict(TypedDict):
    airflow: NotRequired[AirflowConfigDict]
    env: NotRequired[Sequence[KubernetesEnvConfigDict]]
    volumes: NotRequired[Sequence[dict]]
    volumeMounts: NotRequired[Sequence[KubernetesVolumeMountConfigDict]]
    resources: NotRequired[dict]


class MultiKubernetesPipelineConfigDict(TypedDict):
    defaultConfig: NotRequired[KubernetesDefaultConfigDict]
    kubernetesPipelines: Sequence[KubernetesPipelineConfigDict]


class ImportFilesFromKubernetesPipelineConfigDict(TypedDict):
    importFilesFrom: Sequence[str]


KubernetesPipelineFileConfigDict = Union[
    MultiKubernetesPipelineConfigDict,
    ImportFilesFromKubernetesPipelineConfigDict
]
