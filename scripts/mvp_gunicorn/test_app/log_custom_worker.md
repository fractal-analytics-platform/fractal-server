```console
$ poetry run gunicorn --workers=2 main:app -c config.py --worker-class custom_worker.CustomWorker
[2024-05-24 14:36:18 +0200] [259445] [INFO] Starting gunicorn 22.0.0
[2024-05-24 14:36:18 +0200] [259445] [INFO] Listening at: http://127.0.0.1:8000 (259445)
[2024-05-24 14:36:18 +0200] [259445] [INFO] Using worker: custom_worker.CustomWorker
[2024-05-24 14:36:18 +0200] [259449] [INFO] Booting worker with pid: 259449
INIT SIGNALS FROM CUSTOM (self.pid=259449)
[2024-05-24 14:36:19 +0200] [259450] [INFO] Booting worker with pid: 259450
INIT SIGNALS FROM CUSTOM (self.pid=259450)
[2024-05-24 14:36:19 +0200] [259449] [INFO] Started server process [259449]
[2024-05-24 14:36:19 +0200] [259449] [INFO] Waiting for application startup.
STARTUP
[2024-05-24 14:36:19 +0200] [259449] [INFO] Application startup complete.
[2024-05-24 14:36:19 +0200] [259450] [INFO] Started server process [259450]
[2024-05-24 14:36:19 +0200] [259450] [INFO] Waiting for application startup.
STARTUP
[2024-05-24 14:36:19 +0200] [259450] [INFO] Application startup complete.

# NOW SEND SIGABRT

This is from our custom worker_abort, with worker=<custom_worker.CustomWorker object at 0x7231a4baf370>
[2024-05-24 14:36:28 +0200] [259450] [ERROR] Traceback (most recent call last):
  File "/usr/lib/python3.10/asyncio/runners.py", line 44, in run
    return loop.run_until_complete(main)
  File "/usr/lib/python3.10/asyncio/base_events.py", line 636, in run_until_complete
    self.run_forever()
  File "/usr/lib/python3.10/asyncio/base_events.py", line 603, in run_forever
    self._run_once()
  File "/usr/lib/python3.10/asyncio/base_events.py", line 1871, in _run_once
    event_list = self._selector.select(timeout)
  File "/usr/lib/python3.10/selectors.py", line 469, in select
    fd_event_list = self._selector.poll(timeout, max_ev)
  File "/home/tommaso/.cache/pypoetry/virtualenvs/fractal-server-cyevISt_-py3.10/lib/python3.10/site-packages/gunicorn/workers/base.py", line 203, in handle_abort
    sys.exit(1)
SystemExit: 1

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/tommaso/.cache/pypoetry/virtualenvs/fractal-server-cyevISt_-py3.10/lib/python3.10/site-packages/starlette/routing.py", line 741, in lifespan
    await receive()
  File "/home/tommaso/.cache/pypoetry/virtualenvs/fractal-server-cyevISt_-py3.10/lib/python3.10/site-packages/uvicorn/lifespan/on.py", line 137, in receive
    return await self.receive_queue.get()
  File "/usr/lib/python3.10/asyncio/queues.py", line 159, in get
    await getter
asyncio.exceptions.CancelledError

[2024-05-24 14:36:28 +0200] [259450] [INFO] Worker exiting (pid: 259450)
[2024-05-24 14:36:28 +0200] [259445] [ERROR] Worker (pid:259450) exited with code 1
[2024-05-24 14:36:28 +0200] [259445] [ERROR] Worker (pid:259450) exited with code 1.
[2024-05-24 14:36:28 +0200] [259460] [INFO] Booting worker with pid: 259460
INIT SIGNALS FROM CUSTOM (self.pid=259460)
[2024-05-24 14:36:28 +0200] [259460] [INFO] Started server process [259460]
[2024-05-24 14:36:28 +0200] [259460] [INFO] Waiting for application startup.
STARTUP
[2024-05-24 14:36:28 +0200] [259460] [INFO] Application startup complete.
[2024-05-24 14:36:29 +0200] [259445] [INFO] Handling signal: winch
[2024-05-24 14:36:29 +0200] [259445] [INFO] Handling signal: winch

# NOW SEND CTRL-C

^C[2024-05-24 14:36:34 +0200] [259445] [INFO] Handling signal: int
[2024-05-24 14:36:34 +0200] [259449] [INFO] Shutting down
[2024-05-24 14:36:34 +0200] [259460] [INFO] Shutting down
[2024-05-24 14:36:34 +0200] [259449] [INFO] Waiting for application shutdown.
SHUTDOWN
[2024-05-24 14:36:34 +0200] [259449] [INFO] Application shutdown complete.
[2024-05-24 14:36:34 +0200] [259449] [INFO] Finished server process [259449]
[2024-05-24 14:36:34 +0200] [259445] [ERROR] Worker (pid:259449) was sent SIGINT!
[2024-05-24 14:36:34 +0200] [259460] [INFO] Waiting for application shutdown.
SHUTDOWN
[2024-05-24 14:36:34 +0200] [259460] [INFO] Application shutdown complete.
[2024-05-24 14:36:34 +0200] [259460] [INFO] Finished server process [259460]
[2024-05-24 14:36:34 +0200] [259445] [ERROR] Worker (pid:259460) was sent SIGINT!
[2024-05-24 14:36:34 +0200] [259445] [INFO] Shutting down: Master
```
