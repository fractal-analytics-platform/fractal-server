#!/bin/bash

# set -e

# Create and init db
createdb fractal-example-test
uv run --frozen fractalctl set-db
uv run --frozen fractalctl init-db-data --resource default --profile default --admin-email admin@example.org --admin-pwd 1234 --admin-project-dir "$(pwd)/project-dir"

# Start the server
uv run --frozen gunicorn fractal_server.main:app \
    --workers 2 \
    --bind "0.0.0.0:8000" \
    --access-logfile - \
    --error-logfile - \
    --worker-class fractal_server.gunicorn_fractal.FractalWorker \
    --logger-class fractal_server.gunicorn_fractal.FractalGunicornLogger \
