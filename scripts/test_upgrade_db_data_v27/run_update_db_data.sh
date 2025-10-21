#!/bin/bash

set -e

# Create empty fractal_test_v27 db
dropdb --if-exists fractal_test_v27
createdb fractal_test_v27

# Load existing db into fractal_test_v27
PGPASSWORD=postgres psql -U postgres -h localhost -d fractal_test_v27 -f "$1"

# When running with "old" database, this is needed:
./venv260/bin/fractalctl set-db
echo -e "yes\nyes\nyes\nyes\nyes" | ./venv260/bin/fractalctl update-db-data

# Apply 2.7.0 schema migrations
poetry run fractalctl set-db

# Apply 2.7.0 data migrations
./venv270/bin/fractalctl set-db
export FRACTAL_V27_DEFAULT_USER_EMAIL=admin@example.org
echo -e "yes\nyes\nyes\nyes\nyes" | ./venv270/bin/fractalctl update-db-data

# Reset all passwords to a given value
# Raw: 1234
# Hashed: $2b$12$jWECA5mvg2Oom1uQ3iCai.ENHeIiiJ9UEOvXIjog.s/LWWpZtjbpO
COMMAND="UPDATE \"user_oauth\" SET \"hashed_password\" = '\$2b\$12\$jWECA5mvg2Oom1uQ3iCai.ENHeIiiJ9UEOvXIjog.s/LWWpZtjbpO'"
echo "Now running ${COMMAND}"
psql --dbname=fractal_test_v27 --command="$COMMAND"
echo "All passwords have been reset to \"1234\""
