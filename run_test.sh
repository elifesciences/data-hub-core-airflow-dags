#!/bin/bash

set -e

# avoid issues with .pyc/pyo files when mounting source directory
export PYTHONOPTIMIZE=

echo "running tests"
pytest -p no:cacheprovider

echo "running pylint"
PYLINTHOME=/tmp/bigquery-views-pylint \
  pylint bigquery_views tests

echo "running flake8"
flake8 bigquery_views tests

echo "done"