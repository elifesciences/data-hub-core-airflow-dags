#!/bin/bash

set -e
pip install -r requirements.txt

pip install -e . --no-dependencies
cp dags $1 -r


