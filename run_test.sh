#!/bin/bash

set -e

if  [ $1 != "with-end-to-end" ]; then
: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
export AIRFLOW__CORE__FERNET_KEY
airflow initdb
fi

# avoid issues with .pyc/pyo files when mounting source directory
export PYTHONOPTIMIZE=


echo "running unit tests"
pytest tests/unit_test/ -p no:cacheprovider -s --disable-warnings

echo "running dag validation tests"
pytest tests/dag_validation_test/ -p no:cacheprovider -s --disable-warnings

echo "running pylint"
PYLINTHOME=/tmp/datahub-dags-pylint \
 pylint tests/ data_pipeline/

echo "running flake8"
flake8 flake8  tests/ data_pipeline/


if [[ $1  &&  $1 == "with-end-to-end" ]]; then
    echo "running end to end tests"
    pytest tests/end2end_test/ -p no:cacheprovider -s
fi

echo "done"
