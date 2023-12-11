#!/bin/bash

# Sets up a Postgres DB for Igor

set -eu

# Expects env variables:
#
## How to reach postgres
#  - PGHOST
#  - PGPORT
#  - PGUSER
#  - PGPASS
#
## What we're going to set
#  - PGUSER_READWRITE_PASS
#  - PGUSER_READONLY_PASS

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR

# Load env variables
PGHOST=${PGHOST:-localhost}
PGPORT=${PGPORT:-5432}
PGUSER=${PGUSER:-postgres}

# Template user passwords
RW_PASS=${PGUSER_READWRITE_PASS:-readwrite} \
RO_PASS=${PGUSER_READONLY_PASS:-readonly} \
envsubst <00.tmpl >00.sql

# Template user passwords
TABLE_JOBS=${TABLE_JOBS:-Job} \
TABLE_LAYERS=${TABLE_LAYERS:-Layer} \
TABLE_TASKS=${TABLE_TASKS:-Task} \
envsubst <01.tmpl >01.sql

# Run
PGPASSWORD=${PGPASSWORD:-test} psql -h $PGHOST -p $PGPORT -U $PGUSER -d postgres -f 00.sql
PGPASSWORD=${PGUSER_READWRITE_PASS:-readwrite} psql -h $PGHOST -p $PGPORT -U igorreadwrite -d igor -f 01.sql
