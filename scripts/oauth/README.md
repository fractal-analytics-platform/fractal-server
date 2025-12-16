Terminal 1
```
docker compose up -d
uv run --frozen ./start_test_server_locally.sh
```

Terminal 2
```bash
export PGUSER=postgres
export PGPASSWORD=postgres
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=fractal_test

uv run --frozen bash -x oauth.sh
```
