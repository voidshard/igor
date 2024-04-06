#!/bin/bash

# Runs some high level end-to-end tests on Igor services.
#
# Ie. not unit tests, this stands up a redis & postgres in docker to run against.

PGHOST=${PGHOST:-localhost}
PGPORT=${PGPORT:-5432}
PGDATABASE=${PGDATABASE:-igor}

# these test user/passwords are made in migrations/dev/
OWNUSER=${OWNUSER:-postgres} # owner
OWNPASS=${OWNPASS:-test}
RWUSER=${RWUSER:-postgres} # readwrite
RWPASS=${RWPASS:-test}

REDISHOST=${REDISHOST:-localhost}
REDISPORT=${REDISPORT:-6379}
REDISDB=${REDISDB:-0}

APIPORT=${APIPORT:-8100}

set -eux

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR

# build the igor binary so we can access & test the migration tool
IGOR=/tmp/igor
go build -o $IGOR $SCRIPT_DIR/../cmd/igor/*.go

# stand up the test infra
docker compose build
docker compose up -d

# wait for the postgres server to be ready
RETRIES=5
until PGPASSWORD=$OWNPASS psql -h $PGHOST -p $PGPORT -U $OWNUSER -c "select 1" > /dev/null 2>&1 || [ $RETRIES -eq 0 ]; do
  echo "Waiting for postgres server, $((RETRIES--)) remaining attempts..."
  sleep 5
done

# apply db migrations
# Create the DB
DATABASE_URL="postgres://${OWNUSER}:${OWNPASS}@${PGHOST}:${PGPORT}/igor?sslmode=disable"
$IGOR migrate setup

# Apply the migrations
$IGOR migrate up --source file://${SCRIPT_DIR}/../migrations/prod
# Print the version
$IGOR migrate version --source file://${SCRIPT_DIR}/../migrations/prod

# run the tests
set +e 
IGOR_TEST_API="http://localhost:${APIPORT}/api/v1" IGOR_TEST_DATA=${SCRIPT_DIR}/data IGOR_TEST_PG_URL=${DATABASE_URL} IGOR_TEST_RD_URL="redis://${REDISHOST}:${REDISPORT}/${REDISDB}" go test -v ./...

# tear down & remove the test infra
docker compose stop
docker compose rm -f
