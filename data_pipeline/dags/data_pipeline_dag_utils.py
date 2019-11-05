import json
import logging
from datetime import timedelta
from functools import partial
from inspect import getmembers
from urllib.parse import urlparse
from typing import Callable, Iterable

import airflow
from airflow.operators.dagrun_operator import TriggerDagRunOperator, DagRunOrder
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import DAG
from airflow.utils import timezone
from airflow.api.common.experimental.trigger_dag import trigger_dag


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


def parse_gs_url(url):
    parsed_url = urlparse(url)
    if parsed_url.scheme != "gs":
        raise AssertionError("expected gs:// url, but got: %s" % url)
    if not parsed_url.hostname:
        raise AssertionError("url is missing bucket / hostname: %s" % url)
    return {"bucket": parsed_url.hostname,
            "object": parsed_url.path.lstrip("/")}


def _to_absolute_urls(bucket: str, path_iterable: Iterable[str]):
    return [f"gs://{bucket}/{path}" for path in path_iterable]


# OPERATOR CLASS  ========================================================


class AbsoluteUrlGoogleCloudStorageListOperator(
        GoogleCloudStorageListOperator):
    def execute(self, context):
        return _to_absolute_urls(self.bucket, super().execute(context))


# CREATE OPERATORS =======================================================
def create_watch_sensor(dag, task_id, url_prefix, **kwargs):
    parsed_url = parse_gs_url(url_prefix)
    return GoogleCloudStoragePrefixSensor(
        task_id=task_id,
        bucket=parsed_url["bucket"],
        prefix=parsed_url["object"],
        dag=dag,
        **kwargs,
    )


def create_list_operator(dag, task_id, url_prefix):
    parsed_url = parse_gs_url(url_prefix)
    return AbsoluteUrlGoogleCloudStorageListOperator(
        task_id=task_id,
        bucket=parsed_url["bucket"],
        prefix=parsed_url["object"],
        dag=dag,
    )


def create_retrigger_operator(dag, task_id=None):
    if not task_id:
        task_id = f"retrigger_{dag.dag_id}"
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=dag.dag_id,
        dag=dag)


def create_trigger_operator(
    dag: DAG,
    trigger_dag_id: str,
    task_id: str = None,
    python_callable: Callable[[dict, DagRunOrder], DagRunOrder] = None,
    transform_conf: Callable[[dict], dict] = None,
):
    if not task_id:
        task_id = f"trigger_{trigger_dag_id}"
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=trigger_dag_id,
        python_callable=partial(
            _conditionally_trigger_dag,
            trigger_dag_id=trigger_dag_id,
            python_callable=python_callable,
            transform_conf=transform_conf,
        ),
        dag=dag,
    )


def create_trigger_next_task_dag_operator(
    dag: DAG, task_id: str = "trigger_next_task_dag"
):
    return PythonOperator(
        dag=dag,
        task_id=task_id,
        provide_context=True,
        python_callable=_trigger_next_task_fn,
    )


# INSTANTIATE OPERATORS IN DAG ===========================================


def create_watch_and_list_operator(dag, task_id_prefix, url_prefix, **kwargs):
    return create_watch_sensor(dag=dag,
                               task_id=f"{task_id_prefix}_watch",
                               url_prefix=url_prefix,
                               **kwargs) >> create_list_operator(dag=dag,
                                                                 task_id=f"{task_id_prefix}_list",
                                                                 url_prefix=url_prefix)


# OPERATORS  CALLABLES  FOR DAG MGT ======================================


def _conditionally_trigger_dag(
    context: dict,
    dag_run_obj: DagRunOrder,
    trigger_dag_id: str,
    python_callable: Callable[[dict, DagRunOrder], DagRunOrder],
    transform_conf: Callable[[dict], dict] = None,
):
    conf: dict = context["dag_run"].conf
    # the payload is used as the conf for the next dag run, we can just pass
    # it through
    dag_run_obj.payload = conf
    if transform_conf:
        dag_run_obj.payload = transform_conf(conf)
    dag_run_obj.run_id = _get_full_run_id(
        conf=dag_run_obj.payload, default_run_id=dag_run_obj.run_id
    )
    if python_callable:
        dag_run_obj = python_callable(context, dag_run_obj)
    if dag_run_obj:
        LOGGER.info("triggering %s with: %s", trigger_dag_id, conf)
    return dag_run_obj


def _trigger_next_task_fn(**kwargs):
    dag: DAG = kwargs["dag"]
    dag_id = dag.dag_id
    LOGGER.info("current dag id: %s", dag_id)
    conf: dict = kwargs["dag_run"].conf
    tasks: list = conf.get("tasks")
    LOGGER.info("tasks: %s", tasks)
    if not tasks:
        LOGGER.info("no tasks configured, skipping")
        return
    try:
        task_index = tasks.index(dag_id)
    except ValueError:
        raise ValueError(
            'current dag not found in task list, "%s" not in  %s' %
            (dag_id, tasks))
    LOGGER.info("current dag task index: %d", task_index)
    if task_index + 1 == len(tasks):
        LOGGER.info("last tasks, skipping")
        return
    next_task_id = tasks[task_index + 1]
    LOGGER.info("next_task_id: %s", next_task_id)
    simple_trigger_dag(next_task_id, conf)


def simple_trigger_dag(dag_id, conf, suffix=""):
    run_id = _get_full_run_id(
        conf=conf,
        default_run_id=f"trig__{timezone.utcnow().isoformat()}{suffix}")
    trigger_dag(
        dag_id=dag_id,
        run_id=run_id,
        conf=json.dumps(conf),
        execution_date=None,
        replace_microseconds=False,
    )


def _get_full_run_id(conf: dict, default_run_id: str) -> str:
    run_name = conf.get("run_name")
    if run_name:
        return f"{default_run_id}_{run_name}"
    return default_run_id


##################################### HOOKS  AND MACROS MGT ##############


def get_gs_hook(
        google_cloud_storage_conn_id="google_cloud_default",
        delegate_to=None):
    return GoogleCloudStorageHook(
        google_cloud_storage_conn_id=google_cloud_storage_conn_id,
        delegate_to=delegate_to,
    )


def add_dag_macro(dag: DAG, macro_name: str, macro_fn: callable):
    if not dag.user_defined_macros:
        dag.user_defined_macros = {}
    dag.user_defined_macros[macro_name] = macro_fn


def add_dag_macros(dag: DAG, macros: object):
    LOGGER.debug("adding macros: %s", macros)
    for name, value in getmembers(macros):
        if callable(value):
            LOGGER.debug("adding macro: %s", name)
            add_dag_macro(dag, name, value)
        else:
            LOGGER.debug(
                "not adding macro, not callable: %s -> %s",
                name,
                value)


################################# ########################################
