import importlib
import os

import pytest
from airflow import models as af_models
from airflow.utils.dag_cycle_tester import test_cycle

from tests.dag_validation_test.conftest import DAG_FILES, DAG_PATH


@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_should_contain_no_cycle(dag_file):
    module_name, _ = os.path.splitext(dag_file)
    module_path = os.path.join(DAG_PATH, dag_file)

    mod_spec = importlib.util.spec_from_file_location(
        module_name, module_path)

    module = importlib.util.module_from_spec(mod_spec)

    mod_spec.loader.exec_module(module)

    dag_objects = [
        var for var in vars(module).values() if isinstance(
            var, af_models.DAG)]
    assert len(dag_objects) > 0

    for dag in dag_objects:
        test_cycle(dag)


def test_should_successfully_import_all_dags(dagbag):
    assert len(dagbag.import_errors) == 0, \
        f"DAG import failures. Errors: {dagbag.import_errors}"
