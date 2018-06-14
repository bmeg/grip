# ================================
# Downlaod and load example data
# ================================

# curl -L -o postgres_test_data.dump http://cl.ly/173L141n3402/download/example.dump
# export PGPASSWORD=mysecretpassword
# createdb --host localhost -U postgres test
# pg_restore --host localhost -U postgres --no-owner --dbname test postgres_test_data.dump

# ========================
# Sample example data
# ========================

# https://github.com/mla/pg_sample
# pg_sample --host localhost -U postgres test > postgres_smtest_data.sql
# createdb --host localhost -U postgres smtest
# psql --host localhost -U postgres smtest < postgres_smtest_data.sql
# pg_dump --host localhost -U postgres smtest > postgres_smtest_data.dump

# ==============================
# Restore sampled example data
# ==============================

# pg_restore --host localhost -U postgres --no-owner --dbname smtest postgres_smtest_data.dump
