DB_NAME="tmp_fractal_$(date +%s)"
OLD_DB_FILE=./clean_db_fractal_2.6.4.sql
NEW_FRACTAL_SERVER_VERSION=2.7.0a11

echo "DB_NAME: $DB_NAME"

dropdb "$DB_NAME"
createdb "$DB_NAME"
psql -d "$DB_NAME" -f "$OLD_DB_FILE"

python3 -m venv venv
source venv/bin/activate
python3 -m pip install "fractal-server[postgres-psycopg-binary]==$NEW_FRACTAL_SERVER_VERSION"

# Make sure that .fractal_server.env looks similar to
echo "POSTGRES_DB=$DB_NAME" >> .fractal_server.env
