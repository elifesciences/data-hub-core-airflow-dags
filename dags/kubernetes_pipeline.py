# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

from datetime import timedelta
import logging
from typing import Sequence

import airflow
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
)

LOGGER = logging.getLogger(__name__)


def create_kubernetes_pipeline_dags() -> Sequence[airflow.DAG]:
    dags = []
    with create_dag(
        dag_id='kubernetes_pipeline',
        dagrun_timeout=timedelta(days=1),
        tags=['Kubernetes']
    ) as dag:
        image = 'elifesciences/data-hub-core-dags-dev'
        LOGGER.info('image: %r', image)
        k = KubernetesPodOperator(
            name="hello-dry-run",
            image=image,
            cmds=["bash", "-cx"],
            arguments=["sleep 60"],
            labels={"foo": "bar"},
            task_id="dry_run_demo",
            do_xcom_push=False
        )
        dags.append(dag)
    return dags


DAGS = create_kubernetes_pipeline_dags()

FIRST_DAG = DAGS[0]
