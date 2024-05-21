# MVP Gunicorn workers and FastAPI state

Run the main.py FastAPI application with Gunicorn (at least 2 workers):

```bash
poetry run gunicorn -w 2 -k worker_custom.CustomWorker main:app
```

In another terminal run `loop.sh` to populate the state of the different workers.

Now kill one of the two worker with a SIGABRT:

```bash
ps -waux | grep gunicorn

kill -6 $pid
```
