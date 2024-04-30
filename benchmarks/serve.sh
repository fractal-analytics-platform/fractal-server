#!/bin/bash

N_WORKERS=1
BIND=0.0.0.0:8000
WORKER_CLASS=uvicorn.workers.UvicornWorker

gunicorn "fractal_server.main:app" \
    --bind=$BIND \
    --workers=$N_WORKERS \
    --worker-class=$WORKER_CLASS \
    --daemon \
    --access-logfile fractal-server.out \
    --error-logfile fractal-server.err

sleep 2
