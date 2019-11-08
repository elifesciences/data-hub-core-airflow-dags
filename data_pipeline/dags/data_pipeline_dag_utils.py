import logging
from datetime import timedelta

import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone

LOGGER = logging.getLogger(__name__)


def get_default_args():
    return {
        "start_date": airflow.utils.dates.days_ago(1),
        "retries": 10,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "provide_context": True,
    }


def create_python_task(
        dag,
        task_id,
        python_callable,
        trigger_rule='all_success',
        retries=0):
    return PythonOperator(
        task_id=task_id,
        dag=dag,
        python_callable=python_callable,
        trigger_rule=trigger_rule,
        retries=retries)


def get_task_run_instance_fullname(task_context):
    return '___'.join([task_context.get('dag').dag_id, task_context.get(
        'run_id'), task_context.get('task').task_id])
