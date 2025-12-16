# using local postgres database
dropdb fractal_test
createdb fractal_test

# using container postgres created with
# docker run --name postgres-main -p 5432:5432 -e POSTGRES_PASSWORD=postgres -d postgres

# docker exec -it postgres-main dropdb -Upostgres fractal_test
# docker exec -it postgres-main createdb -Upostgres fractal_test

uv run --frozen fractalctl set-db
uv run --frozen python populate_db_script.py
