#!/bin/bash

N_WORKERS=1
BIND=0.0.0.0:8000
WORKER_CLASS=asgi

gunicorn "fractal_server.main:app" \
    --bind=$BIND \
    --workers=$N_WORKERS \
    --worker-class=$WORKER_CLASS \
    --asgi-loop uvloop \
    --daemon \
    --access-logfile fractal-server.out \
    --error-logfile fractal-server.err
