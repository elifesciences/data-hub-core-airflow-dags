#!/bin/bash
# <description>
#
# This file starts a web service that can be used to serve the log files created by airflow worker node/pod/container
# After which it also starts the dask worker service on the worker node/pod/container

set -e

cd $AIRFLOW_HOME/serve
python3 -m http.server 8793 &

cd -
dask-worker $@
