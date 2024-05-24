```console
$ poetry run gunicorn --workers=2 test:app -c config.py --worker-class gevent
[2024-05-24 14:18:02 +0200] [254312] [INFO] Starting gunicorn 22.0.0
[2024-05-24 14:18:02 +0200] [254312] [INFO] Listening at: http://127.0.0.1:8000 (254312)
[2024-05-24 14:18:02 +0200] [254312] [INFO] Using worker: gevent
[2024-05-24 14:18:02 +0200] [254315] [INFO] Booting worker with pid: 254315
[2024-05-24 14:18:02 +0200] [254317] [INFO] Booting worker with pid: 254317
[2024-05-24 14:18:13 +0200] [254312] [INFO] Handling signal: winch
[2024-05-24 14:18:13 +0200] [254312] [INFO] Handling signal: winch

# NOW SEND SIGABRT

This is from our custom worker_abort, with worker=<gunicorn.workers.ggevent.GeventWorker object at 0x7a55529eb6d0>
[2024-05-24 14:18:21 +0200] [254317] [INFO] Worker exiting (pid: 254317)
[2024-05-24 14:18:21 +0200] [254312] [ERROR] Worker (pid:254317) exited with code 1
[2024-05-24 14:18:21 +0200] [254312] [ERROR] Worker (pid:254317) exited with code 1.
[2024-05-24 14:18:21 +0200] [254563] [INFO] Booting worker with pid: 254563

# NOW SEND CTRL-C

^C[2024-05-24 14:18:25 +0200] [254312] [INFO] Handling signal: int
[2024-05-24 14:18:25 +0200] [254315] [INFO] Worker exiting (pid: 254315)
[2024-05-24 14:18:25 +0200] [254563] [INFO] Worker exiting (pid: 254563)
Traceback (most recent call last):
Traceback (most recent call last):
  File "/home/tommaso/.cache/pypoetry/virtualenvs/fractal-server-cyevISt_-py3.10/lib/python3.10/site-packages/gevent/monkey.py", line 849, in _shutdown
    sleep()
  File "/home/tommaso/.cache/pypoetry/virtualenvs/fractal-server-cyevISt_-py3.10/lib/python3.10/site-packages/gevent/hub.py", line 159, in sleep
    waiter.get()
  File "/home/tommaso/.cache/pypoetry/virtualenvs/fractal-server-cyevISt_-py3.10/lib/python3.10/site-packages/gevent/monkey.py", line 849, in _shutdown
    sleep()
  File "src/gevent/_waiter.py", line 143, in gevent._gevent_c_waiter.Waiter.get
  File "/home/tommaso/.cache/pypoetry/virtualenvs/fractal-server-cyevISt_-py3.10/lib/python3.10/site-packages/gevent/hub.py", line 159, in sleep
    waiter.get()
  File "src/gevent/_waiter.py", line 154, in gevent._gevent_c_waiter.Waiter.get
  File "src/gevent/_waiter.py", line 143, in gevent._gevent_c_waiter.Waiter.get
  File "src/gevent/_greenlet_primitives.py", line 61, in gevent._gevent_c_greenlet_primitives.SwitchOutGreenletWithLoop.switch
  File "src/gevent/_waiter.py", line 154, in gevent._gevent_c_waiter.Waiter.get
  File "src/gevent/_greenlet_primitives.py", line 61, in gevent._gevent_c_greenlet_primitives.SwitchOutGreenletWithLoop.switch
  File "src/gevent/_greenlet_primitives.py", line 61, in gevent._gevent_c_greenlet_primitives.SwitchOutGreenletWithLoop.switch
  File "src/gevent/_greenlet_primitives.py", line 65, in gevent._gevent_c_greenlet_primitives.SwitchOutGreenletWithLoop.switch
  File "src/gevent/_greenlet_primitives.py", line 61, in gevent._gevent_c_greenlet_primitives.SwitchOutGreenletWithLoop.switch
  File "src/gevent/_gevent_c_greenlet_primitives.pxd", line 35, in gevent._gevent_c_greenlet_primitives._greenlet_switch
  File "src/gevent/_greenlet_primitives.py", line 65, in gevent._gevent_c_greenlet_primitives.SwitchOutGreenletWithLoop.switch
  File "src/gevent/greenlet.py", line 908, in gevent._gevent_cgreenlet.Greenlet.run
  File "src/gevent/_gevent_c_greenlet_primitives.pxd", line 35, in gevent._gevent_c_greenlet_primitives._greenlet_switch
  File "/home/tommaso/.cache/pypoetry/virtualenvs/fractal-server-cyevISt_-py3.10/lib/python3.10/site-packages/gunicorn/workers/base.py", line 198, in handle_quit
    sys.exit(0)
  File "src/gevent/greenlet.py", line 908, in gevent._gevent_cgreenlet.Greenlet.run
  File "/home/tommaso/.cache/pypoetry/virtualenvs/fractal-server-cyevISt_-py3.10/lib/python3.10/site-packages/gunicorn/workers/base.py", line 198, in handle_quit
    sys.exit(0)
SystemExit: 0
SystemExit: 0
2024-05-24T12:18:25Z <greenlet.greenlet object at 0x7a555275ae40 (otid=0x7a555273b510) current active started main> failed with SystemExit

2024-05-24T12:18:25Z <greenlet.greenlet object at 0x7a5552760480 (otid=0x7a555273f5a0) current active started main> failed with SystemExit

[2024-05-24 14:18:25 +0200] [254312] [INFO] Shutting down: Master



```
