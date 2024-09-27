#!/bin/bash

dropdb --if-exists fractal_test_v27
createdb fractal_test_v27

PGPASSWORD=postgres psql -U postgres -h localhost -d fractal_test_v27 -f ../../tests/data/testing_databases//clean_db_fractal_2.6.0.sql

poetry run fractalctl set-db

echo -e "yes\nyes\nyes\nyes\nyes"| poetry run fractalctl update-db-data
