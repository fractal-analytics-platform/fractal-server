#!/bin/bash

if [ ! -f alembic.ini ]; then
    echo "ERROR: You must run this script from the folder where alembic.ini file is."
    exit 1
fi
if [[ $# -ne 1 ]]; then
    echo "ERROR: Invalid input."
    echo "Expected usage: $0 \"MIGRATION MESSAGE\""
    exit 2
fi


export POSTGRES_DB="autogenerate-fractal-revision"
export POSTGRES_HOST="/var/run/postgresql/"
dropdb --if-exist "$POSTGRES_DB"
createdb "$POSTGRES_DB"
poetry run fractalctl set-db --skip-init-data
poetry run alembic revision --autogenerate -m "$MIGRATION_MSG"
dropdb "$POSTGRES_DB"
