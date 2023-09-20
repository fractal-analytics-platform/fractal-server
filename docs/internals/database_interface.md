# Database Interface

Fractal Server allows either _SQLite_ or _Postgres_ to be
used as database.

The choice and the various configurations are set through appropriate
environment variables
(see [Config](https://fractal-analytics-platform.github.io/configuration/)).

To make database operations verbose, set `DB_ECHO` equal to `true`, `True` or
`1`.

## SQLite

SQLite is the default value for `DB_ENGINE`, but you can also set it explicitly:

```
DB_ENGINE=sqlite
```

You must provide the path to the database file, either absolute or relative:

```
SQLITE_PATH=/path/to/fractal_server.db
```

If the `SQLITE_PATH` file does not yet exist, it will be created by
`fractalctl`.


## Postgres

### Requirements

To use Postgres as database, Fractal Server must be installed with the
`postgres` extra:

```console
$ pip install "fractal-server[postgres]"
```

This will install two additional Python libraries, `asyncpg` and `psycopg2`,
which require the following system dependencies:

- postgresql,
- postgresql-contrib,
- libpq-dev,
- gcc.

We must have an active PostgreSQL service, with an _host_, a _port_ and a
default user (e.g. `postgres`).<br>

Here we create a database using `createdb`:

```console
$ createdb \
    --host=localhost \
    --port=1111 \
    --username=postgres \
    --no-password \
    --owner=fractal_superuser \
    fractal_db
```


### Setup


Before running `fractalctl`, add these variables to the environment
(here we use the values from the Requirements example):

- required:

    ```
    DB_ENGINE=postgres
    POSTGRES_DB=fractal_db
    ```

- optional:

    ```
    POSTGRES_HOST=localhost             # default:  localhost
    POSTGRES_PORT=1111                  # default:  5432
    POSTGRES_USER=fractal_superuser
    POSTGRES_PASSWORD=
    ```

Fractal Server will use a
SQLAlchemy](https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.engine.URL.create)
function to generate the URL to connect to:

```
URL.create(
    drivername="postgresql+asyncpg",
    username=self.POSTGRES_USER,
    password=self.POSTGRES_PASSWORD,
    host=self.POSTGRES_HOST,
    port=self.POSTGRES_PORT,
    database=self.POSTGRES_DB,
)
```

`POSTGRES_HOST` can be either a URL or the path to a UNIX domain socket.

We do not necessarily need to enter a user and password. If not specified,
the system user will be used (i.e. `$ id -un`).


### Dump and restore

To backup data, we use the utilities `pg_dump`, `pg_restore` and `psql`.

It is possible to dump data in various formats:

=== "Plain Text"

    ```console
    $ pg_dump \
        --host=localhost \
        --port=1111 \
        --username=fractal_superuser \
        --format=plain \
        --file=fractal_dump.txt \
        fractal_db
    ```

=== "Tar"

    ```console
    $ pg_dump \
        --host=localhost \
        --port=1111 \
        --username=fractal_superuser \
        --format=tar \
        --file=fractal_dump.tar \
        fractal_db
    ```

=== "Custom"

    ```console
    $ pg_dump \
        --host=localhost \
        --port=1111 \
        --username=fractal_superuser \
        --format=custom \
        --file=fractal_dump.sql \
        fractal_db
    ```

=== "Directory"

    ```console
    $ pg_dump \
        --host=localhost \
        --port=1111 \
        --username=fractal_superuser \
        --format=directory \
        --file=fractal_dump \
        fractal_db
    ```


After creating a new empty database

```console
$ createdb \
    --host=localhost \
    --port=1111 \
    --username=postgres \
    --no-password \
    --owner=fractal_superuser \
    new_fractal_db
```

we can populate it using the dumped data:


=== "Plain Text"
    ```console
    $ psql \
        --host=localhost \
        --port=1111 \
        --username=fractal_superuser \
        --dbname=new_fractal_db < fractal_dump.txt
    ```

=== "Tar"
    ```console
    $ pg_restore \
        --host=localhost \
        --port=1111 \
        --username=fractal_superuser \
        --single-transaction \
        --format=tar \
        --dbname=new_fractal_db \
        fractal_dump.tar
    ```

=== "Custom"
    ```console
    $ pg_restore \
        --host=localhost \
        --port=1111 \
        --username=fractal_superuser \
        --single-transaction \
        --format=custom \
        --dbname=new_fractal_db \
        fractal_dump.sql
    ```

=== "Directory"
    ```console
    $ pg_restore \
        --host=localhost \
        --port=1111 \
        --username=fractal_superuser \
        --single-transaction \
        --format=directory \
        --dbname=new_fractal_db \
        fractal_dump
    ```
