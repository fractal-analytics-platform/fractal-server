*Notes*:
* All `psql` commands below may be different in case you need specific `--username` or `--host` options.
* The procedure below takes some extra step so that the database which is modified is a fresh copy of the existing one.

Procedure

1. Start with a postgresql database `fractal_current`, created with fractal-server 1.3.13.
2. Backup current database with
```bash
pg_dump fractal_current --format=plain --file=/somewhere/backup_fractal_current.sql
```
3. Create a new database (e.g. with `create database fractal_new;` from the `psql` shell), and load the dump into the new database:
```bash
psql --dbname=fractal_new < /somewhere/backup_fractal_current.sql
```
4. Install fractal-server 1.4.0.
5. Prepare an appropriate `.fractal_server.env` file pointing to the new database, e.g.
```bash
DB_ENGINE=postgres
POSTGRES_HOST=/var/run/postgresql
POSTGRES_DB=fractal_new

JWT_SECRET_KEY=somethingverysecret
FRACTAL_TASKS_DIR=Tasks
FRACTAL_RUNNER_BACKEND=local
FRACTAL_RUNNER_WORKING_BASE_DIR=Artifacts
```
5. Apply 1.4.0 database migrations to `fractal_new` via
```bash
fractalctl set-db
```
Starting from this point, the `fractal_new` database can be safely used within fractal-server 1.4.0. One more step is needed to fix some data which are missing/invalid, so that an update to 1.4.1. is possible.

6. From an environment where fractal-server 1.4.0 is installed, and from the same folder where `.fractal_server.env` is located, run
```bash
python fix_db.py
```
At this point, an update to 1.4.1 can be safely performed.
