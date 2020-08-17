import json
from unittest.mock import patch, MagicMock

import pytest
from airflow import models as airflow_models
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

from dags import s3_csv_import_pipeline
from dags.s3_csv_import_pipeline import (
    is_dag_etl_running,
    update_prev_run_id_var_val,
)

from data_pipeline.s3_csv_data.s3_csv_etl import (
    NamedLiterals
)


@pytest.fixture(name="mock_variable_get", autouse=True)
def _variable_get():
    with patch.object(s3_csv_import_pipeline.Variable,
                      "get") as mock:
        yield mock


@pytest.fixture(name="mock_variable_set", autouse=True)
def _variable_set():
    with patch.object(s3_csv_import_pipeline.Variable,
                      "set") as mock:
        yield mock


@pytest.fixture(name="mock_dag_run_find", autouse=True)
def _dag_run_find():
    with patch.object(s3_csv_import_pipeline.DagRun,
                      "find") as mock:
        yield mock


@pytest.fixture(name="mock_dag_run", autouse=True)
def _dag_run():
    with patch.object(s3_csv_import_pipeline,
                      "DagRun") as mock:
        yield mock


@pytest.fixture(name="mock_task_instance", autouse=True)
def _task_instance():
    with patch.object(airflow_models,
                      "TaskInstance") as mock:
        yield mock


class TaskContext:
    def __init__(self):

        self.conf = TaskContext.conf_dict

    conf_dict = {
        "dataPipelineId": "data-pipeline-id",
        "importedTimestampFieldName": "imported_timestamp",
        "objectKeyPattern": [
            "obj_pattern_1*",
            "obj_pattern_2*"
        ],
        "stateFile": {
            "bucketName": "{ENV}_bucket_name",
            "objectName": "{ENV}_object_prefix"
        }
    }
    dag = DAG(dag_id='mock_xcom', start_date=timezone.utcnow())
    exec_date = timezone.utcnow()
    task1 = DummyOperator(task_id='Should_Remaining_Tasks_Execute', dag=dag)
    task_instance = MagicMock(name='mock_task_instance')

    @staticmethod
    def get_context(task_instance=None):
        t_instance = (
            task_instance
            if task_instance
            else TaskContext.task_instance
        )

        context_conf = TaskContext()
        context = {
            NamedLiterals.DAG_RUN: context_conf,
            "ti": t_instance
        }
        return context


@pytest.fixture(name="mock_task_context")
def _mock_task_context():
    return TaskContext().get_context()


class TestEtlDagRunning:

    def test_should_return_false_if_etl_with_same_id_is_running(
            self,
            mock_variable_get,
            mock_dag_run_find,
            mock_dag_run,
            mock_task_context: dict
    ):
        run_id = "run_id"
        mock_dag_run.get_state.return_value = NamedLiterals.DAG_RUNNING_STATUS
        mock_dag_run_find.return_value = [mock_dag_run]

        mock_variable_get.return_value = json.dumps(
            {NamedLiterals.RUN_ID: run_id}
        )
        is_etl_dag_running = is_dag_etl_running(**mock_task_context)
        assert not is_etl_dag_running

    def test_should_return_false_if_etl_with_same_id_is_successful(
            self,
            mock_variable_get,
            mock_dag_run_find,
            mock_dag_run,
            mock_task_context: dict
    ):
        run_id = "run_id"
        mock_dag_run.get_state.return_value = "success"
        mock_dag_run_find.return_value = [mock_dag_run]

        mock_variable_get.return_value = json.dumps(
            {NamedLiterals.RUN_ID: run_id}
        )
        is_etl_dag_running = is_dag_etl_running(**mock_task_context)
        mock_dag_run_find.assert_called()
        assert is_etl_dag_running

    def test_should_return_true_if_no_etl_with_similar_id_is_running(
            self,
            mock_variable_get,
            mock_dag_run_find,
            mock_task_context: dict):
        mock_variable_get.return_value = None
        is_etl_dag_running = is_dag_etl_running(**mock_task_context)
        mock_dag_run_find.assert_not_called()
        assert is_etl_dag_running


class TestUpdateVariableVal:

    def test_should_update_variable_value(
            self,
            mock_task_instance,
            mock_variable_set
    ):
        mock_task_instance.xcom_pull.return_value = TaskContext.conf_dict
        context = TaskContext.get_context(mock_task_instance)
        run_id = "run_id"
        context = {
            **context,
            NamedLiterals.RUN_ID: run_id
        }
        etl_id = TaskContext.conf_dict.get("dataPipelineId")
        update_prev_run_id_var_val(**context)
        mock_variable_set.assert_called()
        mock_variable_set.assert_called_with(
            etl_id,
            json.dumps({NamedLiterals.RUN_ID: run_id})
        )
