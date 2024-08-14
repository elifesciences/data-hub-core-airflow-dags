from data_pipeline.kubernetes.kubernetes_pipeline_config import (
    KubernetesPipelineConfig,
    MultiKubernetesPipelineConfig
)
from data_pipeline.kubernetes.kubernetes_pipeline_config_typing import (
    KubernetesPipelineConfigDict
)

KUBERNETES_PIPELINE_CONFIG_DICT_1: KubernetesPipelineConfigDict = {
    'dataPipelineId': 'data_pipeline_id_1'
}


class TestKubernetesPipelineConfig:
    def test_should_read_data_pipeline_id(self):
        result = KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        assert result.data_pipeline_id == 'data_pipeline_id_1'


class TestMultiKubernetesPipelineConfig:
    def test_should_read_kubernetes_pipeline_configs(self):
        result = MultiKubernetesPipelineConfig.from_dict({
            'kubernetesPipelines': [KUBERNETES_PIPELINE_CONFIG_DICT_1]
        })
        assert result.kubernetes_pipelines == [
            KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        ]
