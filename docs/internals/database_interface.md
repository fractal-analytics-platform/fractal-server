# Database Interface

Fractal Server allows either <ins>SQLite</ins> or <ins>Postgres</ins> to be
used as database.

The choice and the various configurations are set through appropriate
environment variables
(see [Config](http://localhost:8001/configuration/)).

To make database operations verbose, set `DB_ECHO` equal to `true`, `True` or
`1`.

## SQLite

SQLite is the default value for `DB_ENGINE`, but we can also set it explicitly:

```
DB_ENGINE=sqlite
```

You must also provide the path to the database file,
either absolute or relative:

```
SQLITE_PATH=/path/to/fractal_server.db
```

If the `SQLITE_PATH` file does not yet exist, it will be created by
`fractalctl`.


## Postgres

### Docker example

We must have an active PostgreSQL service, with an _host_, a _port_ and a
default user.<br>
Here we start one inside a Docker container:

```console
$ docker run \
    --name fractal_db_container \
    --publish 1111:5432 \
    --env POSTGRES_USER=default_user \
    --env POSTGRES_PASSWORD=default_password \
    --detach postgres
```

We must have a _database_ and (optionally) a _user_ dedicated to Fractal.<br>
Here we create both in the running container, even adding a _password_ for the
user:

```console
$ psql \
    --host=localhost \
    --port=1111 \
    --username default_user \
    --command "CREATE USER fractal_superuser WITH PASSWORD 'fractal_secret';" \
    --command "CREATE DATABASE fractal_db OWNER fractal_superuser;"

Password for user default_user: default_password
CREATE ROLE
CREATE DATABASE
```

### Setup

To use Postgres as database, Fractal Server must be installed with the
`postgres` extra:

```console
$ pip install "fractal-server[postgres]"
```

This will install two additional Python libraries, `asyncpg` and `psycopg2`,
which require some system dependencies:

- postgresql,
- postgresql-contrib,
- libpq-dev,
- gcc.

Before running `fractalctl`, add the following environment variables
(here we use the values from our Docker example):

```
DB_ENGINE=postgres
POSTGRES_DB=fractal_db

POSTGRES_HOST=localhost             # default:  localhost
POSTGRES_PORT=1111                  # default:  5432
POSTGRES_USER=fractal_superuser     # default:  system user (`$ id -un`)
POSTGRES_PASSWORD=fractal_secret    # default:  None
```

Note that providing `POSTGRES_DB` is mandatory, while the other Postgres
variables have default values.

`POSTGRES_HOST` can be either a URL or the path to a UNIX domain socket.

### Dump and restore

To backup data, we use the utilities `pg_dump`, `pg_restore` and `psql`.

It is possible to dump data in various formats:

=== "Plain Text"

    ```console
    $ /usr/local/Cellar/libpq/16.0/bin/pg_dump \
        --host=localhost \
        --port=1111 \
        --username=fractal_superuser \
        --format=plain \
        --file=fractal_dump.txt \
        fractal_db

    Password: fractal_secret
    ```

=== "Tar"

    ```console
    $ /usr/local/Cellar/libpq/16.0/bin/pg_dump \
        --host=localhost \
        --port=1111 \
        --username=fractal_superuser \
        --format=tar \
        --file=fractal_dump.tar \
        fractal_db

    Password: fractal_secret
    ```

=== "Custom"

    ```console
    $ /usr/local/Cellar/libpq/16.0/bin/pg_dump \
        --host=localhost \
        --port=1111 \
        --username=fractal_superuser \
        --format=custom \
        --file=fractal_dump.sql \
        fractal_db

    Password: fractal_secret
    ```

=== "Directory"

    ```console
    $ /usr/local/Cellar/libpq/16.0/bin/pg_dump \
        --host=localhost \
        --port=1111 \
        --username=fractal_superuser \
        --format=directory \
        --file=fractal_dump \
        fractal_db

    Password: fractal_secret
    ```


After creating a new empty database

```console
$ psql \
    --host=localhost \
    --port=1111 \
    --username default_user \
    --command "CREATE DATABASE new_fractal_db OWNER fractal_superuser;"


Password for user default_user: default_password
CREATE DATABASE

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
    $ /usr/local/Cellar/libpq/16.0/bin/pg_restore \
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
    $ /usr/local/Cellar/libpq/16.0/bin/pg_restore \
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
    $ /usr/local/Cellar/libpq/16.0/bin/pg_restore \
        --host=localhost \
        --port=1111 \
        --username=fractal_superuser \
        --single-transaction \
        --format=directory \
        --dbname=new_fractal_db \
        fractal_dump
    ```
