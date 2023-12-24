#!/bin/bash

# Runs some high level end-to-end tests on Igor services.
#
# Ie. not unit tests, this stands up a redis & postgres in docker to run against.

PGHOST=${PGHOST:-localhost}
PGPORT=${PGPORT:-15432}
PGUSER=${PGUSER:-postgres}
PGDATABASE=${PGDATABASE:-igor}
PGPASSWORD=${PGPASSWORD:-test}

REDISHOST=${REDISHOST:-localhost}
REDISPORT=${REDISPORT:-16379}
REDISDB=${REDISDB:-0}

set -eux

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR

MIGRATE=$SCRIPT_DIR/../cmd/db_migrate/run.sh

# stand up the test infra
docker compose up -d

# wait for the postgres server to be ready
RETRIES=5
until PGPASSWORD=$PGPASSWORD psql -h $PGHOST -U $PGUSER -d $PGDATABASE -c "select 1" > /dev/null 2>&1 || [ $RETRIES -eq 0 ]; do
  echo "Waiting for postgres server, $((RETRIES--)) remaining attempts..."
  sleep 5
done

# apply the db migration
PGHOST=$PGHOST PGPORT=$PGPORT PGUSER=$PGUSER PGDATABASE=$PGDATABASE PGPASSWORD=$PGPASSWORD $MIGRATE

# run the tests
set +e 
IGOR_TEST_PG_URL="postgres://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT}/${PGDATABASE}?sslmode=disable&search_path=igor" IGOR_TEST_RD_URL="redis://${REDISHOST}:${REDISPORT}/${REDISDB}" go test -v ./...

# tear down & remove the test infra
docker compose stop
docker compose rm -f
