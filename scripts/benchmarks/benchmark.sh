#!/bin/bash

N_WORKERS=1
BIND=127.0.0.1:8000
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

locust -f api_bench.py \
--host http://127.0.0.1:8000 \
--users 1 \
--spawn-rate 10 \
--run-time 10s \
--headless \
--html test.html
