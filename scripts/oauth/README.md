Terminal 1
```
docker compose up -d
poetry run ./start_test_server_locally.sh
```

Terminal 2
```bash
export PGUSER=postgres
export PGPASSWORD=postgres
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=fractal_test

poetry run bash -x oauth.sh
```
