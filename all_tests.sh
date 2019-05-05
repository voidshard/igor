#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=$DIR/lib/:$DIR/tests/:$PYTHONPATH

# running these in sections helps with docker madness.
python3.6 -m pytest ./tests/unit -vvs
python3.6 -m pytest ./tests/integration/runner/ -vvs
python3.6 -m pytest ./tests/integration/database/ -vvs
python3.6 -m pytest ./tests/integration/scheduler/ -vvs

rm -fr $DIR/.pytest_cache/
