# Note: DagBag.process_file skips files without 'airflow' or 'DAG' in them

from datetime import timedelta
import logging
from typing import Sequence

import airflow
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from data_pipeline.kubernetes.kubernetes_pipeline_config import MultiKubernetesPipelineConfig
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
)
from data_pipeline.utils.pipeline_config import (
    get_deployment_env,
    get_pipeline_config_for_env_name_and_config_parser
)

LOGGER = logging.getLogger(__name__)


class KubernetesPipelineConfigEnvironmentVariables:
    CONFIG_FILE_PATH = 'KUBERNETES_PIPELINE_CONFIG_FILE_PATH'


def get_multi_kubernetes_pipeline_config() -> MultiKubernetesPipelineConfig:
    return get_pipeline_config_for_env_name_and_config_parser(
        KubernetesPipelineConfigEnvironmentVariables.CONFIG_FILE_PATH,
        MultiKubernetesPipelineConfig.from_dict
    )


def create_kubernetes_pipeline_dags() -> Sequence[airflow.DAG]:
    dags = []
    multi_kubernetes_pipeline_config = get_multi_kubernetes_pipeline_config()
    deployment_env = get_deployment_env()
    for kubernetes_pipeline_config in multi_kubernetes_pipeline_config.kubernetes_pipelines:
        airflow_config = kubernetes_pipeline_config.airflow_config
        with create_dag(
            dag_id=kubernetes_pipeline_config.data_pipeline_id,
            dagrun_timeout=timedelta(days=1),
            **airflow_config.dag_parameters
        ) as dag:
            KubernetesPodOperator(
                task_id=deployment_env + '-' + kubernetes_pipeline_config.data_pipeline_id,
                random_name_suffix=True,
                image=kubernetes_pipeline_config.image,
                image_pull_policy=kubernetes_pipeline_config.image_pull_policy,
                arguments=kubernetes_pipeline_config.arguments,
                do_xcom_push=False,
                startup_timeout_seconds=600,
                env_vars=kubernetes_pipeline_config.env,
                volumes=kubernetes_pipeline_config.volumes,
                volume_mounts=kubernetes_pipeline_config.volume_mounts,
                container_resources=kubernetes_pipeline_config.resources,
                **airflow_config.task_parameters
            )
            dags.append(dag)

    return dags


DAGS = create_kubernetes_pipeline_dags()

FIRST_DAG = DAGS[0]
