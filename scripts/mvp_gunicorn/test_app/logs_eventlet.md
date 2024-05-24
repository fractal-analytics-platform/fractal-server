```console
$ poetry run gunicorn --workers=2 test:app -c config.py --worker-class eventlet
[2024-05-24 14:19:48 +0200] [254847] [INFO] Starting gunicorn 22.0.0
[2024-05-24 14:19:48 +0200] [254847] [INFO] Listening at: http://127.0.0.1:8000 (254847)
[2024-05-24 14:19:48 +0200] [254847] [INFO] Using worker: eventlet
[2024-05-24 14:19:48 +0200] [254852] [INFO] Booting worker with pid: 254852
[2024-05-24 14:19:48 +0200] [254853] [INFO] Booting worker with pid: 254853
[2024-05-24 14:19:51 +0200] [254847] [INFO] Handling signal: winch
[2024-05-24 14:19:51 +0200] [254847] [INFO] Handling signal: winch


# NOW SEND SIGABRT

This is from our custom worker_abort, with worker=<gunicorn.workers.geventlet.EventletWorker object at 0x7380b2774ac0>
[2024-05-24 14:20:00 +0200] [254852] [INFO] Worker exiting (pid: 254852)
[2024-05-24 14:20:00 +0200] [254847] [ERROR] Worker (pid:254852) exited with code 1
[2024-05-24 14:20:00 +0200] [254847] [ERROR] Worker (pid:254852) exited with code 1.
[2024-05-24 14:20:00 +0200] [254870] [INFO] Booting worker with pid: 254870
[2024-05-24 14:20:00 +0200] [254847] [INFO] Handling signal: winch
[2024-05-24 14:20:00 +0200] [254847] [INFO] Handling signal: winch

# NOW SEND CTRL-C

^C[2024-05-24 14:20:02 +0200] [254847] [INFO] Handling signal: int
[2024-05-24 14:20:02 +0200] [254853] [INFO] Worker exiting (pid: 254853)
[2024-05-24 14:20:02 +0200] [254870] [INFO] Worker exiting (pid: 254870)
[2024-05-24 14:20:02 +0200] [254847] [INFO] Shutting down: Master
```
