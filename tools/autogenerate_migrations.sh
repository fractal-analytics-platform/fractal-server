#!/bin/bash

set -e

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 \"Message string to use with 'revision'\""
    exit 1
else
    MESSAGE=$1
fi

CURRENT_FOLDER=$(pwd)
cd ../fractal_server

POSTGRES_DB="fractal_autogenerate_migrations_$(date -Iseconds)"

echo "POSTGRES_HOST=/var/run/postgresql
JWT_SECRET_KEY=secretkey
POSTGRES_DB=$POSTGRES_DB" > .fractal_server.env

dropdb --if-exist "$POSTGRES_DB"
createdb "$POSTGRES_DB"
uv run --frozen fractalctl set-db
uvrun --frozen alembic revision --autogenerate -m "$MESSAGE"

rm .fractal_server.env

cd "$CURRENT_FOLDER"

dropdb "$POSTGRES_DB"
