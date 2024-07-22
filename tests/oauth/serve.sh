#!/bin/sh

gunicorn "fractal_server.main:app" --bind=0.0.0.0:8001 --worker-class=uvicorn.workers.UvicornWorker --daemon
