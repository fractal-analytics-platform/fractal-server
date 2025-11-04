Fractal Server is the core ingredient of more deployments of the [Fractal framework](https://fractal-analytics-platform.github.io/), which also includes several other Fractal components (e.g. a web client) and also relies on external resources being availble (e.g. a PostgreSQL database and a SLURM cluster).

Here we just describe the basic procedure for a local deployment.
Any production deployments will require greater attention and detail.

Some examples of typical deployment are available as container-based demos at https://github.com/fractal-analytics-platform/fractal-containers/tree/main/examples.


## Prerequisites

The following will assume that:

- You are using a Python version greater or equal than 3.11
- You are working within an isolated Python environment, for example with `venv`:
  ```
  python3 -m venv venv
  venv/bin/activate
  ```

- You have configured the required environment variables (see [configuration page](configuration.md)).

  If you choose to declare the environment variables using the `.fractal_server.env` file, that file must be placed in the current working directory;

- You have access to a dedicated Postgres database (see the [database page](internals/database_interface.md)).

## How to install

Fractal Server is hosted on [PyPI](https://pypi.org/project/fractal-server), and can be installed with `pip`:
```
pip install fractal-server
```

Fractal Server is also available as a [Conda package](https://anaconda.org/conda-forge/fractal-server), but the PyPI version is the recommended one.

For details on how to install Fractal Server in a development environment see the [Development](development.md) page.


## How to deploy

Installing `fractal-server` will automatically install `fractalctl`, its companion command-line utility that provides the basic commands for deploying Fractal Server.

### 1. Set up the database

> This only requires [DatabaseSettings](http://localhost:8000/configuration/#fractal_server.config._database.DatabaseSettings).

Use the command
```
fractalctl set-db
```
to apply the schema migrations to the database.


### 2. Initialize the database

> This only requires [DatabaseSettings](http://localhost:8000/configuration/#fractal_server.config._database.DatabaseSettings) and, possibly, [FRACTAL_DEFAULT_GROUP_NAME](http://localhost:8000/configuration/#fractal_server.config._main.Settings.FRACTAL_DEFAULT_GROUP_NAME).

With the command
```
fractalctl init-db-data
```
you can do multiple things, depending on the environment variables set and on the flag provided:

  - create the default user group, by settings `FRACTAL_DEFAULT_GROUP_NAME=All`;
  - create the first admin user, by providing the `--admin-*` flags;
  - create the first couple resource/profile and associate users to them, providing `--resource` and `--profile`.


**Help message**
```
usage: fractalctl init-db-data [-h] [--resource RESOURCE] [--profile PROFILE] [--admin-email ADMIN_EMAIL] [--admin-pwd ADMIN_PWD] [--admin-project-dir ADMIN_PROJECT_DIR]

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

### 3. Start the server

Finally, we use the command
```
fractalctl start
```
to start the server using [Uvicorn](https://uvicorn.dev/).

**Help message**

```
usage: fractalctl start [-h] [--host HOST] [-p PORT] [--reload]

Start the server (with uvicorn)

options:
  -h, --help            show this help message and exit
  --host HOST           bind socket to this host (default: 127.0.0.1)
  -p PORT, --port PORT  bind socket to this port (default: 8000)
  --reload              enable auto-reload
```

### 4. Test the server

To verify that the server is up, you can use the `/api/alive/` endpoint - as in
```
curl http://localhost:8000/api/alive/
{"alive":true,"version":"2.17.0"}
```
