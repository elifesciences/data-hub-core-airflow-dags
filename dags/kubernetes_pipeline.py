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
    for kubernetes_pipeline_config in multi_kubernetes_pipeline_config.kubernetes_pipelines:
        with create_dag(
            dag_id=kubernetes_pipeline_config.data_pipeline_id,
            dagrun_timeout=timedelta(days=1),
            tags=['Kubernetes']
        ) as dag:
            KubernetesPodOperator(
                task_id='dry_run_demo',
                name='hello-dry-run',
                image='debian',
                arguments=['sleep 60'],
                do_xcom_push=False
            )
            dags.append(dag)

    return dags


DAGS = create_kubernetes_pipeline_dags()

FIRST_DAG = DAGS[0]
