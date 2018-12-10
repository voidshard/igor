#!/usr/bin/env bash

# Setup pythonpath
# export PYTHONPATH=`./path.sh`

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo $DIR/lib/:$PYTHONPATH
