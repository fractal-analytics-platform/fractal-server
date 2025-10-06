Terminal 1
```
docker run -p 5556:5556 ghcr.io/fractal-analytics-platform/oauth:0.1
```

Terminal 2
```
export FRACTAL_TASKS_DIR_zzz=/dev/fractal/task
export FRACTAL_RUNNER_WORKING_BASE_DIR_zzz=/dev/fractal/base_dir
export FRACTAL_RUNNER_BACKEND=local
export JWT_SECRET_KEY=jwt_secret_key
export JWT_EXPIRE_SECONDS=1000
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export POSTGRES_DB=fractal_test
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export OAUTH_DEXIDP_CLIENT_ID=client_test_id
export OAUTH_DEXIDP_CLIENT_SECRET=client_test_secret
export OAUTH_DEXIDP_REDIRECT_URL=http://localhost:8001/auth/dexidp/callback/
export OAUTH_DEXIDP_OIDC_CONFIGURATION_ENDPOINT=http://127.0.0.1:5556/dex/.well-known/openid-configuration
export PGUSER=postgres
export PGPASSWORD=postgres
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=$POSTGRES_DB
dropdb $POSTGRES_DB
createdb $POSTGRES_DB
fractalctl set-db
fractalctl start --port 8001
```

Terminal 3
```
export PGUSER=postgres
export PGPASSWORD=postgres
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=fractal_test
bash -x oauth.sh
```
