#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export PYTHONPATH=$DIR/lib/:$DIR/tests/:$PYTHONPATH

for var in $*
do
  echo python -m pytest ./tests/$var -vvs
  python3.6 -m pytest ./tests/$var -vvs
done

rm -fr $DIR/.pytest_cache/
