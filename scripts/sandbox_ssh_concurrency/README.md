Terminal 1:
```console
$ poetry run uvicorn main:app
```

Terminal 2:
```console
$ curl http://localhost:8000/ssh
```

Terminal 3:
```console
$ curl http://localhost:8000/alive
```

The alive API call takes 10 seconds, because the worker is fully blocked while running `sleep 10` over SSH.
