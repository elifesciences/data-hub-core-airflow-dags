import os

import pytest
from airflow import models as af_models

DAG_PATH = os.path.join(os.path.dirname(__file__), "../..", "dags")
DAG_FILES = [f for f in os.listdir(DAG_PATH) if f.endswith(".py")]


@pytest.fixture(name="dagbag", scope="session")
def _airflow_dagbag() -> af_models.dagbag:
    return af_models.DagBag(dag_folder=DAG_PATH, include_examples=False)
