Fractal Server is the core ingredient of more deployments of the [Fractal framework](https://fractal-analytics-platform.github.io/), which also includes several other Fractal components (e.g. a web client) and also relies on external resources being availble (e.g. a PostgreSQL database and a SLURM cluster).

Here we do not describe the full procedure for a full-fledged Fractal deployment in detail. Some examples of typical deployment are available as container-based demos at https://github.com/fractal-analytics-platform/fractal-containers/tree/main/examples.


## How to install

> ⚠️  The minimum supported Python version for fractal-server is 3.11.

Fractal Server is hosted on [the PyPI index](https://pypi.org/project/fractal-server), and it can be installed with `pip` via
```
pip install fractal-server
```

For details on how to install Fractal Server in a development environment, see the [Development](development.md) page.

## How to deploy

Here we describe the basic steps for running Fractal Server.

### 1. Set up configuration variables

For this command to work properly, a set of variables need to be specified,
either as enviromnent variables or in a file like `.fractal_server.env`.
An example of such file is
```
JWT_SECRET_KEY=XXX
FRACTAL_RUNNER_BACKEND=local
FRACTAL_TASKS_DIR=/some/path/for/task/environments
FRACTAL_RUNNER_WORKING_BASE_DIR=/some/path/for/job/folders
POSTGRES_DB=fractal-database-name
```


> ⚠️  **`JWT_SECRET_KEY=XXX` must be replaced with a more secure string, that
> should not be disclosed.** ⚠️

More details (including default values) are available in the [Configuration](configuration.md) page.


### 2. Set up the database
After creating a PostgreSQL database for `fractal-server`, and after setting the proper `fractal-server` configuration variables (see the [database page](internals/database_interface.md)), the command
```
fractalctl set-db
```
applies the appropriate schema migrations.

### 3. Start the server

In the environment where Fractal Server is installed, you can run it via [`gunicorn`](https://gunicorn.org) with a command like
```
gunicorn fractal_server.main:app \
    --workers 2 \
    --bind "0.0.0.0:8000" \
    --access-logfile - \
    --error-logfile - \
    --worker-class fractal_server.gunicorn_fractal.FractalWorker \
    --logger-class fractal_server.gunicorn_fractal.FractalGunicornLogger
```
To verify that the server is up, you can use the `/api/alive/` endpoint - as in
```console
$ curl http://localhost:8000/api/alive/
{"alive":true,"version":"2.15.6"}
```
