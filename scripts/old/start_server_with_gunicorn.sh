# Other relevant options:
# Specify IP and port:
#     --bind 127.0.0.1:8000
# Specify number of workers
#     --workers 4

gunicorn fractal_server.main:app \
    --worker-class uvicorn.workers.UvicornWorker \
    --access-logfile=-\
    --workers 2\
    --bind 127.0.0.1:8000
