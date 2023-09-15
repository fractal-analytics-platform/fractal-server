# Database Interface

Fractal Server allows either <ins>SQLite</ins> or <ins>Postgres</ins> to be used as database.

To choose between the two, add these variables to the environment:

=== "SQLite"
    ```console
    DB_ENGINE=sqlite

    SQLITE_PATH=/path/to/fractal_server.db
    ```
=== "Postgres"
    ```console
    DB_ENGINE=postgres

    POSTGRES_DB=fractal_db_name
    ```

To make database operations verbose, set `DB_ECHO` equal to `true`, `True` or `1`.

## Postgres configuration

Postgres databases may require some extra configurations.
