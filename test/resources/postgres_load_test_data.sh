#!/bin/bash

set -e

export PGPASSWORD=mysecretpassword

# ==========================================
# Restore sampled example data if it exists
# ==========================================
if [ -f postgres_smtest_data.dump ]; then
    pg_restore --host localhost -U postgres --no-owner --dbname smtest postgres_smtest_data.dump
    exit 0
fi

# ================================
# Downlaod and load example data
# ================================
# From http://postgresguide.com/setup/example.html#understanding-the-schema
if [ ! -f postgres_test_data.dump ]; then
    curl -L -o postgres_test_data.dump http://cl.ly/173L141n3402/download/example.dump
fi

# create databases - it is ok if these commands fail
set +e
createdb --host localhost -U postgres test
createdb --host localhost -U postgres smtest
set -e

pg_restore --host localhost -U postgres --no-owner --dbname test postgres_test_data.dump

# ========================
# Sample example data
# ========================
# https://github.com/mla/pg_sample
pg_sample --host localhost -U postgres test > postgres_smtest_data.sql
psql --host localhost -U postgres smtest < postgres_smtest_data.sql

# ==============================
# Dump sampled example data
# ==============================
pg_dump --host localhost -U postgres smtest > postgres_smtest_data.dump
