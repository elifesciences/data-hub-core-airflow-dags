#!/bin/bash

set -e

cd $AIRFLOW_HOME/serve
python3 -m http.server 8793 &

cd -
dask-worker $@
