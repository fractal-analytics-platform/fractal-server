

## Preliminary requirements

Running Fractal Server assumes that

1. It has access to a shared filesystem on which it can read and write.
2. It has access to a database (currently supported: `postgres` (recommended)
   and `sqlite`).
3. It has access to one of the supported [computational backends](../internals/runners/).

These requirements are sufficient to use the [local
backend](../internals/runners/local/), while the following additional
requirements are needed to use the [SLURM backend](../internals/runners/slurm):

<ol start="4">
<li> Fractal Server is installed on a SLURM client node, configured to submit
and manage jobs. </li>
<li> The user who runs Fractal Server has appropriate `sudo` privileges, e.g.
to run `sbatch` for other users. </li>
<li> The machine where Fractal Server runs exposes a port (possibly only
visible from a private network) for communicating with the Fractal client.
</li>
</ol>

## How to install

> ⚠️  The minimum supported Python version for fractal-server is 3.9.

Fractal Server is hosted on [the PyPI
index](https://pypi.org/project/fractal-server), and it can be installed with
`pip` via
```
pip install fractal-server
```

Some additional features must be installed as *extras*, e.g. via one of the following
```
pip install fractal-server[slurm]
pip install fractal-server[postgres]
pip install fractal-server[gunicorn]
pip install fractal-server[slurm,postgres]
```

When including the `postgres` extra, the following additional packages should
be available on the system (e.g. through an `apt`-like package manager):
`postgresql, postgresql-contrib, libpq-dev, gcc`.


For details on how to install Fractal Server in a development environment, see
the [Contribute](../contribute) page.

## How to deploy

### Basic procedure

The basic procedure for running Fractal Server consists in setting up some
configuration variables, setting up the database, and then starting the server.

#### 1. Set up configuration variables

For this command to work properly, a set of variables need to be specified,
either as enviromnent variables or in a file like `.fractal_server.env`.
An example of such file is
```
JWT_SECRET_KEY=XXX
SQLITE_PATH=/some/path/to/fractal_server.db
FRACTAL_TASKS_DIR=/some/path/to/the/task/environment/folder
FRACTAL_RUNNER_WORKING_BASE_DIR=some_folder_name
FRACTAL_RUNNER_BACKEND=slurm
FRACTAL_SLURM_CONFIG_FILE=/some/path/to/slurm_config.json
```

> ⚠️  **`JWT_SECRET_KEY=XXX` must be replaced with a more secure string, that
> should not be disclosed.** ⚠️

More details (including default values) are available in the [Configuration](../configuration/) page.


#### 2. Set up the database

The command
```
fractalctl set-db
```
initalizes the database, according to the configuration variables.
When using SQLite, for instance, the database file is created at the
`SQLITE_PATH` path (notice: the parent folder must already exist).

#### 3. Start the server

In the environment where Fractal Server is installed, you can run it via
The command
```
fractalctl start
```
starts the server (with [uvicorn](https://www.uvicorn.org)) and binds it to
the 8000 port on `localhost`.  You can add more options (e.g. to specify a
different port) as in
```
usage: fractalctl start [-h] [--host HOST] [-p PORT] [--reload]

Start the server (with uvicorn)

options:
  -h, --help            show this help message and exit
  --host HOST           bind socket to this host (default: 127.0.0.1)
  -p PORT, --port PORT  bind socket to this port (default: 8000)
  --reload              enable auto-reload
```

Now the server is up, and depending on the intended use case you may have to
create/edit some users - see [the Users page](./users.md).


Notice that you could also use more explicit startup commands, see
[below](../install_and_deploy/#serving-fractal-server-via-gunicorn) for an
example based on Gunicorn.

### Ports

You can get details on the open ports e.g. via `ss -tulwn` or `ss -tulp`. An entry like
```
Netid                   State                     Recv-Q                    Send-Q                                         Local Address:Port                                               Peer Address:Port
tcp                     LISTEN                    0                         128                                                  0.0.0.0:8010                                                    0.0.0.0:*
```
shows that port 8010 is open for all the current virtual network (this means for instance that you can use the client from the login node of a SLURM cluster, from any computing node, or from any machine in the local network or with an active VPN).


### Serving Fractal Server via Gunicorn

Fractal Server is served through [uvicorn](https://www.uvicorn.org) by default, when it is run via the `fractalctl start` command, but different servers can be used.
If Fractal Server is installed with the [`gunicorn`](https://gunicorn.org) extra, for instance, you can use a command like
```
gunicorn fractal_server.main:app --workers 2 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8010 --access-logfile logs/fractal-server.out --error-logfile logs/fractal-server.err
```

### Postgres setup

> See preliminary notes at<br>
> https://github.com/fractal-analytics-platform/fractal-server/issues/388#issuecomment-1366713291.

### Fractal Server as a daemon/service

> See preliminary notes at<br>
> https://github.com/fractal-analytics-platform/fractal-server/issues/388#issuecomment-1366719115.
