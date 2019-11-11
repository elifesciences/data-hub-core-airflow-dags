#!/bin/bash

set -e
ls .
mkdir ./test_dir
mv ./dags ./test_dir
mv ./dag_pipeline_test ./test_dir/dag_pipeline_test
mv ./data_pipeline ./test_dir/data_pipeline
cd ./test_dir


: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
export AIRFLOW__CORE__FERNET_KEY
airflow initdb

# avoid issues with .pyc/pyo files when mounting source directory
export PYTHONOPTIMIZE=


echo "running tests"
pytest -p no:cacheprovider -s --disable-warnings

echo "running pylint"
PYLINTHOME=/tmp/bigquery-views-pylint \
  pylint dag_pipeline_test/ data_pipeline/

echo "running flake8"
flake8 flake8  dag_pipeline_test/ data_pipeline/

echo "done"