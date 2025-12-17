import logging
# import json
from datetime import timedelta
from typing import Optional

import airflow
from airflow.providers.standard.operators.python import PythonOperator
# from airflow.api.common.experimental.trigger_dag import trigger_dag

from data_pipeline.utils.airflow_compat import days_ago


LOGGER = logging.getLogger(__name__)


def get_default_args():
    return {
        "start_date": days_ago(1),
        "retries": 10,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True
    }


def create_dag(
        default_args: Optional[dict] = None,
        catchup: bool = False,
        **kwargs):
    if default_args is None:
        default_args = get_default_args()
    return airflow.DAG(
        default_args=default_args,
        catchup=catchup,
        **kwargs
    )


# pylint: disable=too-many-arguments
def create_python_task(
        dag,
        task_id,
        python_callable,
        trigger_rule="all_success",
        retries=0,
        email_on_failure=False,
        **kwargs
):
    return PythonOperator(
        task_id=task_id,
        dag=dag,
        python_callable=python_callable,
        trigger_rule=trigger_rule,
        retries=retries,
        email_on_failure=email_on_failure,
        **kwargs
    )


def get_task_run_instance_fullname(task_context):
    return "___".join(
        [
            task_context.get("dag").dag_id,
            task_context.get("run_id"),
            task_context.get("task").task_id,
        ]
    )
