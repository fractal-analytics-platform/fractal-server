#!/bin/bash

# Wed 20 Dec 13:31:09 CET 2023
# This is an example of how to programmatically apply migrations and/or fix-db
# scripts with different fractal-server version, which is useful to create DB
# dumps updated as of a given fractal-server version.

INPUT_DUMP=clean_db_fractal_1.4.0a6.sql
OUPUT_DUMP=clean_db_fractal_1.4.1.sql


# DB SETUP
DAY=`date +%Y%m%d`
DB_NAME=tmp_fractal_$DAY
createdb $DB_NAME
psql --dbname $DB_NAME < $INPUT_DUMP
echo "\
DB_ENGINE=postgres
POSTGRES_HOST=localhost
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=$DB_NAME" > .fractal_server.env

# v1.4.0 SETUP
VENV_NAME=venv_1_4_0
python3 -m venv $VENV_NAME
source ${VENV_NAME}/bin/activate
python3 -m pip install fractal-server[postgres]==1.4.0
# v1.4.0 ACTIVITY
fractalctl set-db
# v1.4.0 TEARDOWN
deactivate
rm -r $VENV_NAME

# v1.4.1 SETUP
VENV_NAME=venv_1_4_1
python3 -m venv $VENV_NAME
source ${VENV_NAME}/bin/activate
python3 -m pip install fractal-server[postgres]==1.4.1
# v1.4.1 ACTIVITY
fractalctl set-db
# v1.4.1 TEARDOWN
deactivate
rm -r $VENV_NAME

# DB ACTIVITY
pg_dump --format=plain --file=$OUPUT_DUMP $DB_NAME

# CURRENT-VERSION ACTIVITY (NO SETUP/TEARDOWN)
poetry run fractalctl set-db
poetry run python ../../../scripts/database_updates/from_1.4.1_to_1.4.2/fix_db_add_project_dump.py

# DB ACTIVITY
pg_dump --format=plain --file=current_dump.sql $DB_NAME

# DB TEARDOWN
dropdb $DB_NAME
