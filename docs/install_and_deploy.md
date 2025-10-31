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

For the following commands to work, you must specify a set of variables,
either as environment variables or in a file named `.fractal_server.env`.

See the [Configuration](configuration.md) page for details.


### 2. Set up the database
With the proper configuration variables set and having a PostgreSQL database at your disposal (see the [database page](internals/database_interface.md)), run
```
fractalctl set-db
```
to apply the appropriate schema migrations.

FIXME
If you also want to add the first data to the databse
```
fractalctl init-db-data
```
Here's the documentation
```
fractalctl init-db-data --help
usage: fractalctl init-db-data [-h] [--resource RESOURCE] [--profile PROFILE] [--admin-email ADMIN_EMAIL] [--admin-pwd ADMIN_PWD]
                               [--admin-project-dir ADMIN_PROJECT_DIR]

Populate database with initial data.

options:
  -h, --help            show this help message and exit
  --resource RESOURCE   Either `default` or path to the JSON file of the first resource.
  --profile PROFILE     Either `default` or path to the JSON file of the first profile.
  --admin-email ADMIN_EMAIL
                        Email of the first admin user.
  --admin-pwd ADMIN_PWD
                        Password for the first admin user.
  --admin-project-dir ADMIN_PROJECT_DIR
                        Project_dir for the first admin user.

```


### 3. Initialize the database


### 4. Start the server

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
{"alive":true,"version":"2.17.0"}
```
