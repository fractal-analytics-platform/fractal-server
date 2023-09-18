# Database Interface

Fractal Server allows either <ins>SQLite</ins> or <ins>Postgres</ins> to be used as database.

The choice between the two is made through the environment variable `DB_ENGINE`.

http://localhost:8001/configuration/

To make database operations verbose, set `DB_ECHO` equal to `true`, `True` or `1`.

## SQLite

SQLite is the default choice for `DB_ENGINE`.

```
DB_ENGINE=sqlite
```

We must also provide a `SQLITE_PATH` to the database file,
either absolute or relative to ???.

```
SQLITE_PATH=/path/to/fractal_server.db
```

If the `SQLITE_PATH` file does not yet exist, it will be created and populated by `fractalctl set-db`.


## Postgres

PostgreSQL is a more robust database but it requires some more
```
DB_ENGINE=postgres
```

### Setup

We must have an active PostgreSQL service, with an _host_, a _port_ and a default user (e.g. `postgres`).<br>
Here we start one inside a Docker container:

```console
$ docker run \
    --name fractal_db_container \
    --publish 1111:5432 \
    --env POSTGRES_USER=postgres \
    --env POSTGRES_PASSWORD=password \
    --detach postgres
```

We must have a _database_ and (optionally) a _user_ dedicated to Fractal.<br>
Here we create both in the running container, even adding a _password_ for the user:

```console
$ docker exec fractal_db_container psql \
    --username postgres \
    --command "CREATE USER fractal_superuser WITH PASSWORD 'fractal_secret';" \
    --command "CREATE DATABASE fractal_db OWNER fractal_superuser;"
```

---

To use Postgres as database, Fractal Server must be installed with the `postgres` extra:

```console
$ pip install "fractal-server[postgres]"
```

This will install two additional Python libraries, `asyncpg` and `psycopg2`,
which require some system dependencies:

- postgresql,
- postgresql-contrib,
- libpq-dev,
- gcc.

Before running `fractalctl`, add the following Postgres-related environment
variables to the configuration (here we use the values from our Docker example):

```
POSTGRES_DB=fractal_db

POSTGRES_HOST=localhost             # default:  localhost
POSTGRES_PORT=1111                  # default:  5432
POSTGRES_USER=fractal_superuser     # default:  system user (`$ id -un`)
POSTGRES_PASSWORD=fractal_secret    # default:  None
```

Note that providing `POSTGRES_DB` is mandatory, while the other variables have default values.

`POSTGRES_HOST` can be either a URL or the path to a UNIX domain socket.

Finally:
```console
$ fractalctl set-db
$ fractalctl start
```

### Dump and restore
