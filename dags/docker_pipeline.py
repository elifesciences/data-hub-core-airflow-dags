# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

from datetime import timedelta
import logging
import os
from typing import Sequence

import airflow
from airflow.providers.docker.operators.docker import DockerOperator

from docker.types import Mount

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
)

LOGGER = logging.getLogger(__name__)


def get_container_host_project_path() -> str:
    return os.environ['CONTAINER_HOST_PROJECT_PATH']


def create_docker_pipeline_dags() -> Sequence[airflow.DAG]:
    dags = []
    container_host_project_path = get_container_host_project_path()
    with create_dag(
        dag_id='docker_pipeline',
        dagrun_timeout=timedelta(days=1),
        tags=['Docker'],
        schedule=None
    ) as dag:
        image = 'elifesciences/data-hub-core-dags-dev'
        LOGGER.info('image: %r', image)
        mounts = [
            Mount(
                target='/opt/airflow/data_pipeline',
                source=f'{container_host_project_path}/data_pipeline',
                type='bind',
                read_only=True
            )
        ]
        LOGGER.info('mounts: %r', mounts)

        DockerOperator(
            container_name="hello-dry-run",
            image=image,
            command=["bash", "-cx", "sleep 60"],
            task_id="dry_run_demo",
            mounts=mounts,
            mount_tmp_dir=False,
            auto_remove='force',
            do_xcom_push=False
        )
        dags.append(dag)
    return dags


DAGS = create_docker_pipeline_dags()

FIRST_DAG = DAGS[0]
