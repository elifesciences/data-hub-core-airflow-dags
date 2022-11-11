import os

import pytest
from airflow.models import dagbag

DAG_PATH = os.path.join(os.path.dirname(__file__), "../..", "dags")
DAG_FILES = [
    f
    for f in os.listdir(DAG_PATH)
    if f.endswith(".py") and not f.startswith("_")
]


@pytest.fixture(name="dagbag", scope="session")
def _airflow_dagbag() -> dagbag.DagBag:
    return dagbag.DagBag(dag_folder=DAG_PATH, include_examples=False)
