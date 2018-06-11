curl -L -o postgres_test_data.dump http://cl.ly/173L141n3402/download/example.dump
export PGPASSWORD=mysecretpassword
createdb --host localhost -U postgres test
pg_restore --host localhost -U postgres --no-owner --dbname test postgres_test_data.dump
# psql --host localhost -U postgres --dbname pgguide
