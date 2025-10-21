NEW_DB_FILE=./clean_db_fractal_2.7.0a11.tmp.sql

fractalctl set-db
FRACTAL_V27_DEFAULT_USER_EMAIL=admin@example.org fractalctl update-db-data  # and answer yes

pg_dump --format=plain --file="$NEW_DB_FILE" "$DB_NAME"
