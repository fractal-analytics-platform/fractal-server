#!/bin/bash

N_WORKERS=1
BIND=0.0.0.0:8000
WORKER_CLASS=uvicorn.workers.UvicornWorker

fractalctl set-db

gunicorn "fractal_server.main:app" \
    --bind=$BIND \
    --workers=$N_WORKERS \
    --worker-class=$WORKER_CLASS \
    --daemon

# let gunicorn takes its time
sleep 3

python populate_db.py
