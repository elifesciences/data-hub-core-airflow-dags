from data_pipeline.kubernetes.kubernetes_pipeline_config import (
    KubernetesPipelineConfig,
    MultiKubernetesPipelineConfig
)
from data_pipeline.kubernetes.kubernetes_pipeline_config_typing import (
    KubernetesPipelineConfigDict
)

KUBERNETES_PIPELINE_CONFIG_DICT_1: KubernetesPipelineConfigDict = {
    'dataPipelineId': 'data_pipeline_id_1',
    'image': 'image_1',
    'arguments': ['argument_1', 'argument_2']
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


class TestMultiKubernetesPipelineConfig:
    def test_should_read_kubernetes_pipeline_configs(self):
        result = MultiKubernetesPipelineConfig.from_dict({
            'kubernetesPipelines': [KUBERNETES_PIPELINE_CONFIG_DICT_1]
        })
        assert result.kubernetes_pipelines == [
            KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        ]
