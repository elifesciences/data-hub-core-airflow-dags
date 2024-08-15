from data_pipeline.kubernetes.kubernetes_pipeline_config import (
    KubernetesPipelineConfig,
    MultiKubernetesPipelineConfig
)
from data_pipeline.kubernetes.kubernetes_pipeline_config_typing import (
    KubernetesPipelineConfigDict,
    KubernetesVolumeMountConfigDict
)

KUBERNETES_VOLUME_MOUNT_CONFIG_DICT_1: KubernetesVolumeMountConfigDict = {
    'name': 'volume_mount_name_1',
    'mounthPath': 'volume_mount_path_1',
    'readOnly': True
}

KUBERNETES_VOLUME_CONFIG_DICT_1 = {
    'name': 'volume_name_1',
    'secret': {'secretName': 'secret_name_1'}
}

KUBERNETES_PIPELINE_CONFIG_DICT_1: KubernetesPipelineConfigDict = {
    'dataPipelineId': 'data_pipeline_id_1',
    'image': 'image_1',
    'arguments': ['argument_1', 'argument_2'],
    'volumeMounts': [KUBERNETES_VOLUME_MOUNT_CONFIG_DICT_1],
    'volumes': [KUBERNETES_VOLUME_CONFIG_DICT_1]
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
        assert result.arguments == 'argument_1 argument_2'

    def test_should_read_volume_mount(self):
        result = KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        assert result.volume_mounts == [KUBERNETES_VOLUME_MOUNT_CONFIG_DICT_1]

    def test_should_read_volume(self):
        result = KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        assert result.volumes == [KUBERNETES_VOLUME_CONFIG_DICT_1]

class TestMultiKubernetesPipelineConfig:
    def test_should_read_kubernetes_pipeline_configs(self):
        result = MultiKubernetesPipelineConfig.from_dict({
            'kubernetesPipelines': [KUBERNETES_PIPELINE_CONFIG_DICT_1]
        })
        assert result.kubernetes_pipelines == [
            KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        ]
