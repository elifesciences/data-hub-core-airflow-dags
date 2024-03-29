#!/bin/bash

set -e

: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
export AIRFLOW__CORE__FERNET_KEY

# to initialize SQLite DB for running non-e2e test and Postgres DB for running e2e test
# airflow db init

# avoid issues with .pyc/pyo files when mounting source directory
export PYTHONOPTIMIZE=


echo "running unit tests"
pytest tests/unit_test/ -p no:cacheprovider -s --disable-warnings

echo "running dag validation tests"
pytest tests/dag_validation_test/ -p no:cacheprovider -s --disable-warnings

echo "running pylint"
PYLINTHOME=/tmp/datahub-dags-pylint \
    pylint tests/ data_pipeline/ dags/

echo "running flake8"
flake8 tests/ data_pipeline/ dags/

echo "running mypy"
mypy tests/ data_pipeline/ dags/


if [[ $1  &&  $1 == "with-end-to-end" ]]; then
    echo "running end to end tests"
    pytest tests/end2end_test/ -p no:cacheprovider -s
fi

echo "done"
