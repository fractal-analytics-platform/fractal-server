#!/bin/bash

# set -e

# Create and init db
createdb fractal-example-test
poetry run fractalctl set-db
poetry run fractalctl init-db-data --resource default --profile default --admin-email admin@example.org --admin-pwd 1234 --admin-project-dir "$(pwd)/project-dir"

# Start the server
poetry run gunicorn fractal_server.main:app \
    --workers 2 \
    --bind "0.0.0.0:8000" \
    --access-logfile - \
    --error-logfile - \
    --worker-class fractal_server.gunicorn_fractal.FractalWorker \
    --logger-class fractal_server.gunicorn_fractal.FractalGunicornLogger \
