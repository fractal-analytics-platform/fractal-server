gunicorn fractal_server.main:app --worker-class uvicorn.workers.UvicornWorker --access-logfile=-
