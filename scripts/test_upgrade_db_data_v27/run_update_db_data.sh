#!/bin/bash

set -e

# Create empty fractal_test_v27 db
dropdb --if-exists fractal_test_v27
createdb fractal_test_v27

# Load existing db into fractal_test_v27
PGPASSWORD=postgres psql -U postgres -h localhost -d fractal_test_v27 -f "$1"

# Apply 2.6.0 schema migrations
./venv260/bin/fractalctl set-db

# Apply 2.6.0 data migrations
echo -e "yes\nyes\nyes\nyes\nyes" | ./venv260/bin/fractalctl update-db-data

# Apply 2.7.0 schema migrations
poetry run fractalctl set-db

# Specific fix
psql -d fractal_test_v27 -c "update user_oauth set username = 'admin_123' where id=21;"

# Apply 2.7.0 data migrations
echo -e "yes\nyes\nyes\nyes\nyes" | poetry run fractalctl update-db-data --dry-run
