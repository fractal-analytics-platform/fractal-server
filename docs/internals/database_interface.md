# Database Interface

Fractal Server allows either _SQLite_ or _PostgreSQL_ to be used as database,
and the choice is based on the
[DB_ENGINE](../../configuration/#fractal_server.config.Settings.DB_ENGINE)
environment variable; the other database-related configuration variables for
each case are described below (and in the [configuration
page](../../configuration/)).


To make database operations verbose, set `DB_ECHO` equal to `true`, `True` or
`1`.

## SQLite

SQLite is the default value for `DB_ENGINE`, but you can also set it explicitly:

```
DB_ENGINE=sqlite
```

You must always provide the path to the database file, either as absolute or relative path:
```
SQLITE_PATH=/path/to/fractal_server.db
```
If the `SQLITE_PATH` file does not exist, it will be created by `fractalctl
set-db`.

> ⚠️  **Important warnings:**
>
> 1. SQLite is supported but discouraged in production. Given its partial
>    support for ForeignKey constraints, consistency of the database cannot be
>    guaranteed.
> 2. The minimal required version for the sqlite system library is v3.37 (which
>    is for instance the one in the
>    [libsqlite3-0 package](https://packages.ubuntu.com/jammy/libsqlite3-0)
>    package for Ubuntu 22.04LTS).


## PostgreSQL

### Requirements

To use PostgreSQL as a database, Fractal Server must be installed with the
`postgres` extra:

```console
$ pip install "fractal-server[postgres]"
```
which will install two additional Python libraries (`asyncpg` and `psycopg2`).

**NOTE**: the following system libraries are required:

- `postgresql`,
- `postgresql-contrib`,
- `libpq-dev`,
- `gcc`.


### Setup

We assume that a PostgreSQL is active, with some _host_ (this can be e.g.
`localhost` or a UNIX socket like `/var/run/postgresql/`), a _port_ (we use the
default 5432 in the examples below) and a user (e.g. `postgres` or `fractal`).

> Note that a machine user may or may not require a password (e.g.  depending
> on whether the machine username matches with the PostgreSQL username, and on
> whether connection happens via a UNIX socket). See documentation here:
> https://www.postgresql.org/docs/current/auth-pg-hba-conf.html.

Here we create a database called `fractal_db`, through the `createdb` command:

```console
$ createdb \
    --host=localhost \
    --port=5432 \
    --username=postgres \
    --no-password \
    --owner=fractal \
    fractal_db
```

All options of this command (and of the ones below) should be aligned with the
configuration of a specific PostgreSQL instance. Within `fractal-server`, this
is done by setting the following configuration variables (before running
`fractalctl set-db` or `fractalctl start`):

- Required:

    ```
    DB_ENGINE=postgres
    POSTGRES_DB=fractal_db
    ```

- Optional:

    ```
    POSTGRES_HOST=localhost             # default: localhost
    POSTGRES_PORT=5432                  # default: 5432
    POSTGRES_USER=fractal               # example: fractal
    POSTGRES_PASSWORD=
    ```

`fractal-server` will then use the [`URL.create`
function](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.engine.URL.create)
from `SQLalchemy` to generate the appropriate URL to connect to:

```python
URL.create(
    drivername="postgresql+asyncpg",
    username=self.POSTGRES_USER,
    password=self.POSTGRES_PASSWORD,
    host=self.POSTGRES_HOST,
    port=self.POSTGRES_PORT,
    database=self.POSTGRES_DB,
)
```
Note that `POSTGRES_HOST` can be either a URL or the path to a UNIX domain socket (e.g.
`/var/run/postgresql`).


### Backup and restore

To backup and restore data, one can use the utilities `pg_dump`, `pg_restore`
and `psql`.

It is possible to dump/restore data in various formats (see [documentation of
`pg_dump`](https://www.postgresql.org/docs/current/app-pgdump.html)), but in
this example we stick with the default plain-text format.

```console
$ pg_dump \
    --host=localhost \
    --port=5432\
    --username=fractal \
    --format=plain \
    --file=fractal_dump.sql \
    fractal_db
```

In order to restore a database from a dump, we first create a new empty one
(`new_fractal_db`):
```console
$ createdb \
    --host=localhost \
    --port=5432\
    --username=postgres \
    --no-password \
    --owner=fractal \
    new_fractal_db
```
and then we populate it using the dumped data:

```console
$ psql \
    --host=localhost \
    --port=5432\
    --username=fractal \
    --dbname=new_fractal_db < fractal_dump.sql
```

> One of the multiple ways to compress data is to use `gzip`, by adapting the
> commands above as in:
> ```console
> $ pg_dump ... | gzip -c fractal_dump.sql.gz
> $ gzip --decompress --keep fractal_dump.sql.gz
> $ createdb ...
> $ psql ... < fractal_dump.sql
> ```
