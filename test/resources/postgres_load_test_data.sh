#!/bin/bash

set -e
set -x

export PGPASSWORD=

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

function createDB {
    if psql --host localhost --port 15432 -U postgres -lqt | cut -d \| -f 1 | grep -qw $1; then
        dropdb --host localhost --port 15432 -U postgres $1
        createdb --host localhost --port 15432 -U postgres $1
    else
        createdb --host localhost --port 15432 -U postgres $1
    fi
}

# ==========================================
# Restore sampled example data if it exists
# ==========================================
if [ -f $DIR/postgres_smtest_data.dump ]; then
    createDB smtest
    psql --host localhost --port 15432 -U postgres smtest < $DIR/postgres_smtest_data.dump
    exit 0
fi

# ================================
# Downlaod and load example data
# ================================
# From http://postgresguide.com/setup/example.html#understanding-the-schema
if [ ! -f $DIR/postgres_test_data.dump ]; then
    curl -L -o $DIR/postgres_test_data.dump http://cl.ly/173L141n3402/download/example.dump
fi

createDB test
pg_restore --host localhost --port 15432 -U postgres --no-owner --dbname test $DIR/postgres_test_data.dump

# ========================
# Sample example data
# ========================
# https://github.com/mla/pg_sampl
createDB smtest
pg_sample --host localhost --port 15432 -U postgres test > $DIR/postgres_smtest_data.sql
psql --host localhost --port 15432 -U postgres smtest < $DIR/postgres_smtest_data.sql

# ==============================
# Dump sampled example data
# ==============================
pg_dump --host localhost --port 15432 -U postgres smtest > $DIR/postgres_smtest_data.dump
