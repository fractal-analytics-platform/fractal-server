To configure the Fractal Server one must define some environment variables.
Some of them are mandatory, and for security reasons the server will not start
unless they are set. Some are optional and sensible defaults are provided.

> The mandatory variables are the following
>
> `JWT_SECRET_KEY`<br>
> `FRACTAL_TASKS_DIR`<br>
> `FRACTAL_RUNNER_WORKING_BASE_DIR`
>
> together with the database-specific variables, i.e. `SQLITE_PATH` for SQLite, or `DB_ENGINE=postgres` and `POSTGRES_DB` for Postgres.

::: fractal_server.config
