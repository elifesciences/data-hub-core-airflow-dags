#!/bin/bash

set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

pip install -r $DIR/requirements.txt

pip install -e $DIR/ --no-dependencies
cp $DIR/dags $1 -r


