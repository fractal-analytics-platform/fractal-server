Terminal 1
```
docker run -p 5556:5556 ghcr.io/fractal-analytics-platform/oauth:0.1
```

Terminal 2:
```
docker run -p 1025:1025 -p 8025:8025 axllent/mailpit
```

Terminal 3
```
poetry run ./start_test_server_locally.sh
```

Terminal 4
```bash
export PGUSER=postgres
export PGPASSWORD=postgres
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=fractal_test

poetry run bash -x oauth.sh
```
