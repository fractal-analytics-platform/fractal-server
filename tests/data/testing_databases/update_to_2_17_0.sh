#!/bin/bash

set -e

export OLD_DB_VERSION="2.16.6"
export NEW_DB_VERSION="2.17.0"

export POSTGRES_DB=fractal_test
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres

# Save current branch
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

# Checkout to new tag
git checkout "tags/$NEW_DB_VERSION"

# Load old DB
dropdb ${POSTGRES_DB} --if-exists
createdb ${POSTGRES_DB}
psql --dbname=${POSTGRES_DB} < "clean_db_fractal_$OLD_DB_VERSION.sql"

echo "POSTGRES_DB=$POSTGRES_DB" > .fractal_server.env
echo "JWT_SECRET_KEY=fake" >> .fractal_server.env

# Update database schemas
poetry run fractalctl set-db

# Update database data
echo -e "yes\nyes\nyes\nyes\nyes" | poetry run fractalctl update-db-data

# Clean up env file
rm .fractal_server.env

# Dump up-to-data database and remove old one
pg_dump "$POSTGRES_DB" --format=plain --file="clean_db_fractal_${NEW_DB_VERSION}.sql"
rm clean_db_fractal_${OLD_DB_VERSION}.sql

# Get back to current branch
git switch "$CURRENT_BRANCH"
