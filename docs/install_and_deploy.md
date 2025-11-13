Fractal Server is the core ingredient of the [Fractal framework](https://fractal-analytics-platform.github.io/), which includes several other Fractal components (e.g. a web client) and also relies on external resources being available (e.g. a PostgreSQL database and a SLURM cluster).

This page describes the basic procedure to setup and maintain a _local_ `fractal-server` deployment, which is useful for testing and development, but a full-fledged deployment involves many more aspects. Some examples of deployment setups are available as container-based demos at https://github.com/fractal-analytics-platform/fractal-containers/tree/main/examples.


## Prerequisites

The following will assume that:

- You are using a Python version greater or equal than 3.11
- You are working within an isolated Python environment, for example a virtual environment created through `venv` as in
  ```bash
  python3 -m venv venv
  ./venv/bin/activate
  ```

- You have configured the required environment variables (see [configuration page](configuration.md)).

- If you choose to declare the environment variables using the `.fractal_server.env` file, that file must be placed in the current working directory;

- You have access to a dedicated PostgreSQL database (see the [database page](internals/database.md)).

- A few common command-line tools are available, including `du`, `find`, `cut`, `cat`, `wc`, `bash`, `ls`, `tar` and `unzip`.

## Install

Fractal Server is hosted on [PyPI](https://pypi.org/project/fractal-server), and can be installed with `pip`:
```
pip install fractal-server
```

Fractal Server is also available as a [Conda package](https://anaconda.org/conda-forge/fractal-server), but the PyPI version is the recommended one.

For details on how to install Fractal Server in a development environment see the [Development](development.md) page.


## How to deploy

Installing `fractal-server` will automatically install [`fractalctl`](./cli_reference.md#fractalctl), its companion command-line utility that provides the basic commands for deploying Fractal Server.

### Set up database schemas

Use the command
```
fractalctl set-db
```
to apply the schema migrations to the database. This command uses the configuration variables described in [DatabaseSettings](configuration.md/#fractal_server.config._database.DatabaseSettings) - notably including the database name `POSTGRES_DB`.

> **Note**: the corresponding PostgreSQL database must already exist, since it won't be created by `fractalctl set-db`. You can often create it directly through [`createdb`](https://www.postgresql.org/docs/current/app-createdb.html).


### Initialize database data

With the command
```
fractalctl init-db-data
```
you can initialize several relevant database tables. Its behaviors depends on the environment variables and command-line arguments (see the [`fractalctl init-db-data` documentation](./cli_reference.md#fractalctl-init-db-data)), and it can optionally

  - create the default user group (if `FRACTAL_DEFAULT_GROUP_NAME=All`);
  - create the first admin user, by providing the `--admin-*` flags;
  - create the first resource/profile pair and associate users to them, providing `--resource` and `--profile`.


### Start `fractal-server`

Use the command
```
fractalctl start
```
to start the server using [Uvicorn](https://uvicorn.dev/).

To verify that the server is up, you can use the `/api/alive/` endpoint - as in
```
curl http://localhost:8000/api/alive/
{"alive":true,"version":"2.17.0"}
```

### Upgrade `fractal-server`

The high-level procedure for upgrading `fractal-server` on an existing instance is as follows:

* Stop the running `fractal-server` process.
* Create a backup dump of the current database data (see [database page](internals/database.md/#backup-and-restore)).
* Review the [CHANGELOG](changelog.md), and check whether this version upgrade requires any special procedure.
* Upgrade `fractal-server` (e.g. as in `pip install fractal-server==1.2.3`).
* Update the database schemas (as in `fractalctl set-db`).
* If the CHANGELOG requires it, run the data-migration command (`fractalctl update-db-data`). Depending on the specific upgrade, this may require additional actions or information.
* Restart the `fractal-server` process.
