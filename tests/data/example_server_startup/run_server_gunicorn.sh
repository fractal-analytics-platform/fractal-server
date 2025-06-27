#!/bin/bash

# Create and init db
createdb fractal-examples-test
poetry run fractalctl set-db

# Start the server
poetry run gunicorn fractal_server.main:app \
    --workers 1 \
    --worker-class uvicorn.workers.UvicornWorker \
    --bind 0.0.0.0:8000 \
    --access-logfile - \
    --error-logfile - \
    --logger-class fractal_server.gunicorn_fractal.FractalGunicornLogger \
