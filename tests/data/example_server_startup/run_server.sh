#!/bin/bash

# Create an empty db
poetry run fractalctl set-db

# Start the server
poetry run fractalctl start --port 8000
# poetry run gunicorn fractal_server.main:app --workers 8 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000 --access-logfile fractal-server.out --error-logfile fractal-server.err
