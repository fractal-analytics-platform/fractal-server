#!/bin/bash

# Create and init db
createdb fractal-example-test
poetry run fractalctl set-db

# Start the server
poetry run gunicorn fractal_server.main:app \
    --workers 2 \
    --bind "0.0.0.0:8000" \
    --access-logfile - \
    --error-logfile - \
    --worker-class fractal_server.gunicorn_fractal.FractalWorker \
    --logger-class fractal_server.gunicorn_fractal.FractalGunicornLogger \
