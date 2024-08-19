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

KUBERNETES_V1_VOLUME_1 = k8s_models.v1_volume.V1Volume(
    name=KUBERNETES_VOLUME_CONFIG_DICT_1['name'],
    secret=k8s_models.V1SecretVolumeSource(
        secret_name='secret_name_1'
    )
)

KUBERNETES_ENV_CONFIG_DICT_1: KubernetesEnvConfigDict = {
    'name': 'env_name_1',
    'value': 'env_value_1'
}

KUBERNETES_V1_ENV_1 = k8s_models.v1_env_var.V1EnvVar(
    name=KUBERNETES_ENV_CONFIG_DICT_1['name'],
    value=KUBERNETES_ENV_CONFIG_DICT_1['value']
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


class TestMultiKubernetesPipelineConfig:
    def test_should_read_kubernetes_pipeline_configs(self):
        result = MultiKubernetesPipelineConfig.from_dict({
            'kubernetesPipelines': [KUBERNETES_PIPELINE_CONFIG_DICT_1]
        })
        assert result.kubernetes_pipelines == [
            KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        ]
