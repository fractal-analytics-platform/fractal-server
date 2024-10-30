Here is an example of how to add a new testing database, e.g. to create the `clean_db_fractal_2.4.0.sql` based on `clean_db_fractal_2.0.4.sql`.

Note, as usual, that the `psql` and `createdb` may require additional options for authorization.


1. Load existing database into postgres
```bash
DB_NAME="tmp_fractal_$(date +%s)"
OLD_DB_FILE=./clean_db_fractal_2.0.4.sql
NEW_FRACTAL_SERVER_VERSION=2.4.0

echo "DB_NAME: $DB_NAME"

dropdb "$DB_NAME"
createdb "$DB_NAME"
psql -d "$DB_NAME" -f "$OLD_DB_FILE"


python3 -m venv venv
source venv/bin/activate
python3 -m pip install "fractal-server==$NEW_FRACTAL_SERVER_VERSION"

# Make sure that .fractal_server.env looks similar to
echo "POSTGRES_DB=$DB_NAME" >> .fractal_server.env
```

2. Manually edit `.fractal_server.env`, so that there is a single `POSTGRES_DB`
   line. Also make sure that `DB_ENGINE=postgres-psycopg`.

3. Run migrations and dump db to file:
```bash
NEW_DB_FILE=./clean_db_fractal_2.4.0-tmp.sql

fractalctl set-db
fractalctl update-db-data  # and answer yes

pg_dump --format=plain --file="$NEW_DB_FILE" $DB_NAME
```
