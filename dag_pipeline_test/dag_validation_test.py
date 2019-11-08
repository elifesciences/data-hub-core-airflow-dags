import importlib
import os
import pytest
from airflow import models as af_models


DAG_PATH = os.path.join(
            os.path.dirname(__file__), '..', 'data_pipeline', 'dags'
            )

DAG_FILES = [f for f in os.listdir(DAG_PATH) if f.endswith('pipeline.py')]


class TestCrossrefDag:

    @pytest.mark.parametrize('dag_file', DAG_FILES)
    def test_dag_integrity(self, dag_file):
        """Import dag files and check for DAG."""
        module_name, _ = os.path.splitext(dag_file)
        module_path = os.path.join(DAG_PATH, dag_file)
        mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(mod_spec)
        mod_spec.loader.exec_module(module)

        dag_objects = [var for var in vars(module).values() if isinstance(var, af_models.DAG)]
        assert len(dag_objects)

        for dag in dag_objects:
            dag.test_cycle()

    def test_import_dags(self):
        dagbag = af_models.DagBag(dag_folder=DAG_PATH)
        assert not len(dagbag.import_errors), 'DAG import failures. Errors: {}'.format(dagbag.import_errors)
