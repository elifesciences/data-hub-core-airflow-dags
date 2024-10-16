import logging
import json
from datetime import timedelta
from typing import Optional

import airflow
from airflow.operators.python import PythonOperator
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.utils import timezone

from data_pipeline.utils.pipeline_config import ConfigKeys


LOGGER = logging.getLogger(__name__)


def get_default_args():
    return {
        "start_date": airflow.utils.dates.days_ago(1),
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


def simple_trigger_dag(dag_id, conf: dict, suffix=''):
    run_id = _get_full_run_id(
        conf=conf,
        default_run_id=f'trig__{timezone.utcnow().isoformat()}{suffix}'
    )
    trigger_dag(
        dag_id=dag_id,
        run_id=run_id,
        conf=json.dumps(conf),
        execution_date=None,
        replace_microseconds=False
    )


def get_suffix_for_config(config: dict) -> str:
    config_id = config.get(ConfigKeys.DATA_PIPELINE_CONFIG_ID)
    return '_' + config_id if config_id else ''


def trigger_data_pipeline_dag(dag_id, conf: dict, suffix=None):
    if suffix is None:
        suffix = get_suffix_for_config(conf)
    simple_trigger_dag(dag_id, conf, suffix=suffix)


def _get_full_run_id(conf: dict, default_run_id: str) -> str:
    run_name = conf.get('run_name')
    if run_name:
        return truncate_run_id(f'{default_run_id}_{run_name}')
    return truncate_run_id(default_run_id)


def truncate_run_id(run_id: str) -> str:
    # maximum is 250
    return run_id[:250]
