#!/bin/bash

# Create empty fractal_test_v27 db
dropdb --if-exists fractal_test_v27
createdb fractal_test_v27

# Load existing db into fractal_test_v27
PGPASSWORD=postgres psql -U postgres -h localhost -d fractal_test_v27 -f "$1"

# Apply 2.6.0 schema migrations
./venv2.6.0/bin/fractalctl set-db

# Apply 2.6.0 date migrations
echo -e "yes\nyes\nyes\nyes\nyes" | ./venv2.6.0/bin/fractalctl update-db-data


poetry run fractalctl set-db

echo -e "yes\nyes\nyes\nyes\nyes" | poetry run fractalctl update-db-data --dry-run
