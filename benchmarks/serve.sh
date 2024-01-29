#!/bin/bash

N_WORKERS=1
BIND=0.0.0.0:8000
WORKER_CLASS=uvicorn.workers.UvicornWorker

fractalctl set-db

python ../scripts/populate_db/populate_script_v2.py

gunicorn "fractal_server.main:app" \
    --bind=$BIND \
    --workers=$N_WORKERS \
    --worker-class=$WORKER_CLASS \
    --daemon
