from kubernetes.client import models as k8s_models

from data_pipeline.kubernetes.kubernetes_pipeline_config import (
    KubernetesPipelineConfig,
    MultiKubernetesPipelineConfig
)
from data_pipeline.kubernetes.kubernetes_pipeline_config_typing import (
    KubernetesEnvConfigDict,
    KubernetesPipelineConfigDict,
    KubernetesVolumeMountConfigDict
)
from data_pipeline.utils.pipeline_config import AirflowConfig
from data_pipeline.utils.pipeline_config_typing import AirflowConfigDict


AIRFLOW_CONFIG_DICT_1: AirflowConfigDict = {
    'dagParameters': {'dag_param_1': 'dag value 1'},
    'taskParameters': {'task_param_1': 'task value 1'},
}


KUBERNETES_VOLUME_MOUNT_CONFIG_DICT_1: KubernetesVolumeMountConfigDict = {
    'name': 'volume_mount_name_1',
    'mountPath': 'volume_mount_path_1',
    'readOnly': True
}

KUBERNETES_V1_VOLUME_MOUNT_1 = k8s_models.v1_volume_mount.V1VolumeMount(
    name=KUBERNETES_VOLUME_MOUNT_CONFIG_DICT_1['name'],
    mount_path=KUBERNETES_VOLUME_MOUNT_CONFIG_DICT_1['mountPath'],
    read_only=KUBERNETES_VOLUME_MOUNT_CONFIG_DICT_1['readOnly']
)

KUBERNETES_VOLUME_CONFIG_DICT_1 = {
    'name': 'volume_name_1',
    'secret': {'secretName': 'secret_name_1'}
}

KUBERNETES_V1_VOLUME_1 = k8s_models.V1Volume(
    name=KUBERNETES_VOLUME_CONFIG_DICT_1['name'],
    secret=k8s_models.V1SecretVolumeSource(
        secret_name='secret_name_1'
    )
)

KUBERNETES_ENV_CONFIG_DICT_1: KubernetesEnvConfigDict = {
    'name': 'env_name_1',
    'value': 'env_value_1'
}

KUBERNETES_V1_ENV_1 = k8s_models.V1EnvVar(
    name=KUBERNETES_ENV_CONFIG_DICT_1['name'],
    value=KUBERNETES_ENV_CONFIG_DICT_1['value']
)

KUBERNETES_RESOURCES_CONFIG_DICT_1 = {
    'limits': {'memory': '1Gi', 'cpu': '10m'},
    'requests': {'memory': '1Gi', 'cpu': '10m'}
}

KUBERNETES_V1_RESOURCES_1 = k8s_models.V1ResourceRequirements(
    limits=KUBERNETES_RESOURCES_CONFIG_DICT_1['limits'],
    requests=KUBERNETES_RESOURCES_CONFIG_DICT_1['requests']
)

KUBERNETES_PIPELINE_CONFIG_DICT_1: KubernetesPipelineConfigDict = {
    'dataPipelineId': 'data_pipeline_id_1',
    'image': 'image_1',
    'arguments': ['argument_1', 'argument_2'],
    'volumeMounts': [KUBERNETES_VOLUME_MOUNT_CONFIG_DICT_1],
    'volumes': [KUBERNETES_VOLUME_CONFIG_DICT_1],
    'env': [KUBERNETES_ENV_CONFIG_DICT_1]
}


class TestKubernetesPipelineConfig:
    def test_should_read_data_pipeline_id(self):
        result = KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        assert result.data_pipeline_id == 'data_pipeline_id_1'

    def test_should_read_airflow_config(self):
        result = KubernetesPipelineConfig.from_dict({
            **KUBERNETES_PIPELINE_CONFIG_DICT_1,
            'airflow': AIRFLOW_CONFIG_DICT_1
        })
        assert result.airflow_config == AirflowConfig.from_dict(AIRFLOW_CONFIG_DICT_1)

    def test_should_read_image(self):
        result = KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        assert result.image == 'image_1'

    def test_should_read_arguments(self):
        result = KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        assert result.arguments == ['argument_1', 'argument_2']

    def test_should_read_volume_mount(self):
        result = KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        assert result.volume_mounts == [KUBERNETES_V1_VOLUME_MOUNT_1]

    def test_should_read_volume(self):
        result = KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        assert result.volumes == [KUBERNETES_V1_VOLUME_1]

    def test_should_read_env(self):
        result = KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        assert result.env == [KUBERNETES_V1_ENV_1]

    def test_should_read_resources(self):
        result = KubernetesPipelineConfig.from_dict({
            **KUBERNETES_PIPELINE_CONFIG_DICT_1,
            'resources': KUBERNETES_RESOURCES_CONFIG_DICT_1
        })
        assert result.resources == KUBERNETES_V1_RESOURCES_1


class TestMultiKubernetesPipelineConfig:
    def test_should_read_kubernetes_pipeline_configs(self):
        result = MultiKubernetesPipelineConfig.from_dict({
            'kubernetesPipelines': [KUBERNETES_PIPELINE_CONFIG_DICT_1]
        })
        assert result.kubernetes_pipelines == [
            KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        ]
