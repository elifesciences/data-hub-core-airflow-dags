import json
from pathlib import Path

from kubernetes.client import models as k8s_models

from data_pipeline.kubernetes.kubernetes_pipeline_config import (
    KubernetesPipelineConfig,
    KubernetesPipelineConfigEnvironmentVariables,
    KubernetesPipelineFileConfig,
    get_multi_kubernetes_pipeline_config
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

KUBERNETES_ENV_CONFIG_DICT_2: KubernetesEnvConfigDict = {
    'name': 'env_name_2',
    'value': 'env_value_2'
}

KUBERNETES_V1_ENV_1 = k8s_models.V1EnvVar(
    name=KUBERNETES_ENV_CONFIG_DICT_1['name'],
    value=KUBERNETES_ENV_CONFIG_DICT_1['value']
)

KUBERNETES_V1_ENV_2 = k8s_models.V1EnvVar(
    name=KUBERNETES_ENV_CONFIG_DICT_2['name'],
    value=KUBERNETES_ENV_CONFIG_DICT_2['value']
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
    'arguments': ['argument_1', 'argument_2']
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
        result = KubernetesPipelineConfig.from_dict({
            **KUBERNETES_PIPELINE_CONFIG_DICT_1,
            'volumeMounts': [KUBERNETES_VOLUME_MOUNT_CONFIG_DICT_1]
        })
        assert result.volume_mounts == [KUBERNETES_V1_VOLUME_MOUNT_1]

    def test_should_override_volume_mounths(self):
        result = KubernetesPipelineConfig.from_dict(
            {
                **KUBERNETES_PIPELINE_CONFIG_DICT_1,
                'volumeMounts': [{
                    'name': 'volume_mount_name_2',
                    'mountPath': 'volume_mount_path_1',
                    'readOnly': False
                }, {
                    'name': 'new_volume_mount_name',
                    'mountPath': 'new_volume_mount_path',
                    'readOnly': True
                }]
            },
            default_config_dict={
                'volumeMounts': [{
                    'name': 'unchanged_volume_mount_name',
                    'mountPath': 'unchanged_volume_mount_path',
                    'readOnly': True
                }, {
                    'name': 'volume_mount_name_1',
                    'mountPath': 'volume_mount_path_1',
                    'readOnly': False
                }]
            }
        )
        assert result.volume_mounts == [
            k8s_models.v1_volume_mount.V1VolumeMount(
                name='unchanged_volume_mount_name',
                mount_path='unchanged_volume_mount_path',
                read_only=True
            ),
            k8s_models.v1_volume_mount.V1VolumeMount(
                name='volume_mount_name_2',
                mount_path='volume_mount_path_1',
                read_only=False
            ),
            k8s_models.v1_volume_mount.V1VolumeMount(
                name='new_volume_mount_name',
                mount_path='new_volume_mount_path',
                read_only=True
            )
        ]

    def test_should_read_volume(self):
        result = KubernetesPipelineConfig.from_dict({
            **KUBERNETES_PIPELINE_CONFIG_DICT_1,
            'volumes': [KUBERNETES_VOLUME_CONFIG_DICT_1]
        })
        assert result.volumes == [KUBERNETES_V1_VOLUME_1]

    def test_should_override_volumes(self):
        result = KubernetesPipelineConfig.from_dict(
            {
                **KUBERNETES_PIPELINE_CONFIG_DICT_1,
                'volumes': [{
                    'name': 'volume_name_1',
                    'secret': {'secretName': 'updated_secret_name'}
                }, {
                    'name': 'new_volume_name',
                    'secret': {'secretName': 'new_secret_name'}
                }]
            },
            default_config_dict={
                'volumes': [{
                    'name': 'unchanged_volume_name',
                    'secret': {'secretName': 'unchanged_secret_name'}
                }, {
                    'name': 'volume_name_1',
                    'secret': {'secretName': 'original_secret_name'}
                }]
            }
        )
        assert result.volumes == [
            k8s_models.V1Volume(
                name='unchanged_volume_name',
                secret=k8s_models.V1SecretVolumeSource(
                    secret_name='unchanged_secret_name'
                )
            ),
            k8s_models.V1Volume(
                name='volume_name_1',
                secret=k8s_models.V1SecretVolumeSource(
                    secret_name='updated_secret_name'
                )
            ),
            k8s_models.V1Volume(
                name='new_volume_name',
                secret=k8s_models.V1SecretVolumeSource(
                    secret_name='new_secret_name'
                )
            )
        ]

    def test_should_read_env(self):
        result = KubernetesPipelineConfig.from_dict({
            **KUBERNETES_PIPELINE_CONFIG_DICT_1,
            'env': [KUBERNETES_ENV_CONFIG_DICT_1]
        })
        assert result.env == [KUBERNETES_V1_ENV_1]

    def test_should_use_env_variables_from_default_config(self):
        assert 'env' not in KUBERNETES_PIPELINE_CONFIG_DICT_1
        result = KubernetesPipelineConfig.from_dict(
            KUBERNETES_PIPELINE_CONFIG_DICT_1,
            default_config_dict={
                'env': [KUBERNETES_ENV_CONFIG_DICT_1]
            }
        )
        assert result.env == [KUBERNETES_V1_ENV_1]

    def test_should_combined_env_variables_from_default_and_pipeline_config(self):
        result = KubernetesPipelineConfig.from_dict(
            {
                **KUBERNETES_PIPELINE_CONFIG_DICT_1,
                'env': [KUBERNETES_ENV_CONFIG_DICT_2]
            },
            default_config_dict={
                'env': [KUBERNETES_ENV_CONFIG_DICT_1]
            }
        )
        assert result.env == [
            KUBERNETES_V1_ENV_1,
            KUBERNETES_V1_ENV_2
        ]

    def test_should_override_env_variable(self):
        result = KubernetesPipelineConfig.from_dict(
            {
                **KUBERNETES_PIPELINE_CONFIG_DICT_1,
                'env': [{
                    'name': 'env_1',
                    'value': 'updated-value'
                }]
            },
            default_config_dict={
                'env': [{
                    'name': 'env_1',
                    'value': 'original-value'
                }]
            }
        )
        assert result.env == [
            k8s_models.V1EnvVar(
                name='env_1',
                value='updated-value'
            )
        ]

    def test_should_read_resources(self):
        result = KubernetesPipelineConfig.from_dict({
            **KUBERNETES_PIPELINE_CONFIG_DICT_1,
            'resources': KUBERNETES_RESOURCES_CONFIG_DICT_1
        })
        assert result.resources == KUBERNETES_V1_RESOURCES_1

    def test_should_use_default_resources_if_not_defined_in_pipeline(self):
        result = KubernetesPipelineConfig.from_dict(
            KUBERNETES_PIPELINE_CONFIG_DICT_1,
            default_config_dict={
                'resources': {
                    'limits': {'memory': '1Gi', 'cpu': '10m'},
                    'requests': {'memory': '1Gi', 'cpu': '10m'}
                }
            }
        )
        assert result.resources == k8s_models.V1ResourceRequirements(
            limits={'memory': '1Gi', 'cpu': '10m'},
            requests={'memory': '1Gi', 'cpu': '10m'}
        )

    def test_should_overwrite_default_resources_if_defined_in_pipeline(self):
        result = KubernetesPipelineConfig.from_dict(
            {
                **KUBERNETES_PIPELINE_CONFIG_DICT_1,
                'resources': {
                    'limits': {'memory': '2Gi', 'cpu': '20m'},
                    'requests': {'memory': '2Gi', 'cpu': '20m'}
                }
            },
            default_config_dict={
                'resources': {
                    'limits': {'memory': '1Gi', 'cpu': '10m'},
                    'requests': {'memory': '1Gi', 'cpu': '10m'}
                }
            }
        )
        assert result.resources == k8s_models.V1ResourceRequirements(
            limits={'memory': '2Gi', 'cpu': '20m'},
            requests={'memory': '2Gi', 'cpu': '20m'}
        )


class TestKubernetesPipelineFileConfig:
    def test_should_read_kubernetes_pipeline_configs(self):
        result = KubernetesPipelineFileConfig.from_dict({
            'kubernetesPipelines': [KUBERNETES_PIPELINE_CONFIG_DICT_1]
        })
        assert result.kubernetes_pipelines == [
            KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        ]

    def test_should_return_empty_list_if_import_from_files_is_empty_list(self):
        result = KubernetesPipelineFileConfig.from_dict({
            'importFromFiles': []
        })
        assert result.kubernetes_pipelines == []

    def test_should_import_from_file_using_absolute_path(self, tmp_path: Path):
        config_file_path_1 = tmp_path / 'config-file1.yaml'
        config_file_path_1.write_text(json.dumps({
            'kubernetesPipelines': [KUBERNETES_PIPELINE_CONFIG_DICT_1]
        }), encoding='utf-8')
        result = KubernetesPipelineFileConfig.from_dict({
            'importFromFiles': [str(config_file_path_1)]
        })
        assert result.kubernetes_pipelines == [
            KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        ]

    def test_should_import_from_file_using_relative_path(self, tmp_path: Path):
        config_file_path_1 = tmp_path / 'config-file1.yaml'
        config_file_path_1.write_text(json.dumps({
            'kubernetesPipelines': [KUBERNETES_PIPELINE_CONFIG_DICT_1]
        }), encoding='utf-8')
        result = KubernetesPipelineFileConfig.from_dict({
            'importFromFiles': [config_file_path_1.name]
        }, base_path=str(tmp_path))
        assert result.kubernetes_pipelines == [
            KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        ]

    def test_should_read_default_airflow_pipeline_configs(self):
        assert 'airflow' not in KUBERNETES_PIPELINE_CONFIG_DICT_1
        result = KubernetesPipelineFileConfig.from_dict({
            'defaultConfig': {
                'airflow': AIRFLOW_CONFIG_DICT_1
            },
            'kubernetesPipelines': [KUBERNETES_PIPELINE_CONFIG_DICT_1]
        })
        assert len(result.kubernetes_pipelines) == 1
        assert result.kubernetes_pipelines[0].airflow_config == (
            AirflowConfig.from_dict(AIRFLOW_CONFIG_DICT_1)
        )

    def test_should_override_default_airflow_config_with_part_exists_in_pipeline_config(self):
        result = KubernetesPipelineFileConfig.from_dict({
            'defaultConfig': {
                'airflow': AIRFLOW_CONFIG_DICT_1
            },
            'kubernetesPipelines': [{
                **KUBERNETES_PIPELINE_CONFIG_DICT_1,
                'airflow': {
                    'dagParameters': {'scheduler': '@hourly'}
                }
            }]
        })
        assert len(result.kubernetes_pipelines) == 1
        assert result.kubernetes_pipelines[0].airflow_config == (
            AirflowConfig.from_dict({
                **AIRFLOW_CONFIG_DICT_1,
                'dagParameters': {
                    **AIRFLOW_CONFIG_DICT_1['dagParameters'],
                    'scheduler': '@hourly'
                }
            })
        )


class TestGetMultiKubernetesPipelineConfig:
    def test_should_read_from_config_file_referenced_by_env_name(
        self,
        tmp_path: Path,
        mock_env: dict
    ):
        config_file_path_1 = tmp_path / 'config-file1.yaml'
        config_file_path_1.write_text(json.dumps({
            'kubernetesPipelines': [KUBERNETES_PIPELINE_CONFIG_DICT_1]
        }), encoding='utf-8')
        mock_env[KubernetesPipelineConfigEnvironmentVariables.CONFIG_FILE_PATH] = (
            str(config_file_path_1)
        )
        result = get_multi_kubernetes_pipeline_config()
        assert result.kubernetes_pipelines == [
            KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        ]

    def test_should_read_from_config_file_with_import_from_files_referenced_by_env_name(
        self,
        tmp_path: Path,
        mock_env: dict
    ):
        config_file_path_1 = tmp_path / 'config-file1.yaml'
        config_file_path_1.write_text(json.dumps({
            'kubernetesPipelines': [KUBERNETES_PIPELINE_CONFIG_DICT_1]
        }), encoding='utf-8')
        config_file_path_2 = tmp_path / 'config-file2.yaml'
        config_file_path_2.write_text(json.dumps({
            'importFromFiles': [str(config_file_path_1)]
        }), encoding='utf-8')
        mock_env[KubernetesPipelineConfigEnvironmentVariables.CONFIG_FILE_PATH] = (
            str(config_file_path_2)
        )
        result = get_multi_kubernetes_pipeline_config()
        assert result.kubernetes_pipelines == [
            KubernetesPipelineConfig.from_dict(KUBERNETES_PIPELINE_CONFIG_DICT_1)
        ]
