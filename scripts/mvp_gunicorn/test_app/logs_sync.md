```console
poetry run gunicorn --workers=2 test:app -c config.py --worker-class sync

[2024-05-24 14:15:44 +0200] [253458] [INFO] Starting gunicorn 22.0.0
[2024-05-24 14:15:44 +0200] [253458] [INFO] Listening at: http://127.0.0.1:8000 (253458)
[2024-05-24 14:15:44 +0200] [253458] [INFO] Using worker: sync
[2024-05-24 14:15:44 +0200] [253463] [INFO] Booting worker with pid: 253463
[2024-05-24 14:15:44 +0200] [253464] [INFO] Booting worker with pid: 253464
[2024-05-24 14:15:53 +0200] [253458] [INFO] Handling signal: winch
[2024-05-24 14:15:53 +0200] [253458] [INFO] Handling signal: winch

# NOW SEND SIGABRT

This is from our custom worker_abort, with worker=<gunicorn.workers.sync.SyncWorker object at 0x71e148b7fdf0>
[2024-05-24 14:16:10 +0200] [253463] [INFO] Worker exiting (pid: 253463)
[2024-05-24 14:16:10 +0200] [253458] [ERROR] Worker (pid:253463) exited with code 1
[2024-05-24 14:16:10 +0200] [253458] [ERROR] Worker (pid:253463) exited with code 1.
[2024-05-24 14:16:10 +0200] [253832] [INFO] Booting worker with pid: 253832

# NOW SEND CTRL-C

^C[2024-05-24 14:16:13 +0200] [253458] [INFO] Handling signal: int
[2024-05-24 14:16:13 +0200] [253832] [INFO] Worker exiting (pid: 253832)
[2024-05-24 14:16:13 +0200] [253464] [INFO] Worker exiting (pid: 253464)
[2024-05-24 14:16:13 +0200] [253458] [INFO] Shutting down: Master
```
