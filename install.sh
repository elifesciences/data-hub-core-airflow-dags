#!/bin/bash

# <description>
#
# This file is used to install the data pipeline files in this repository into the
# data hub image (https://github.com/elifesciences/data-hub-airflow-image) created
# which is meant to be deployed for use in the k8s deployment of airflow

set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

pip install  --user -r $DIR/requirements.txt

pip install -e $DIR/ --user --no-dependencies
cp $DIR/dags $1 -r
