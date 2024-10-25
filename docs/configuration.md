To configure the Fractal Server one must define some environment variables.
Some of them are required, and the server will not start unless they are set.
Some are optional and sensible defaults are provided.

> The required variables are the following
>
> ```
> JWT_SECRET_KEY
> FRACTAL_TASKS_DIR
> FRACTAL_RUNNER_WORKING_BASE_DIR
> ```
>
> together with the database-specific variables: `SQLITE_PATH` for SQLite, or `DB_ENGINE=postgres-psycopg` and `POSTGRES_DB` for Postgres.

::: fractal_server.config
