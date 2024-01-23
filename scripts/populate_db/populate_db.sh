dropdb fractal_test
createdb fractal_test
poetry run fractalctl set-db
poetry run python populate_script_v2.py
