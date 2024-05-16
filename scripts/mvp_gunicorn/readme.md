# MVP Gunicorn workers and FastAPI state

Run the main.py FastAPI application with Gunicorn (at least 2 workers):

```bash
poetry run gunicorn -w 2 -k uvicorn.workers.UvicornWorker main:app
```

In another terminal run `loop.sh` to populate the state of the different workers.
The expected behaviour is something like this:

```bash
{"jobs":[1]}
{"jobs":[1,2]}
{"jobs":[1,2,3]}
{"jobs":[1,2,3,4]}
{"jobs":[1,2,3,4,5]}
{"jobs":[6]}
{"jobs":[6,7]}
{"jobs":[6,7,8]}
{"jobs":[1,2,3,4,5,9]} # first worker state
{"jobs":[6,7,8,10]} # second worker state
```

Now kill one of the two worker:

```bash
ps -waux | grep gunicorn

kill -s sigterm $pid
```

Rerun the `loop.sh` and you should see that one worker keeps the previous state, the killed one has a fresh new state.
