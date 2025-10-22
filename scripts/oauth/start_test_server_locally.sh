#!/bin/bash

export FRACTAL_RUNNER_BACKEND=local
export JWT_SECRET_KEY=jwt_secret_key
export JWT_EXPIRE_SECONDS=1000

export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export POSTGRES_DB=fractal_test
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432

export OAUTH_CLIENT_NAME="dexidp"
export OAUTH_CLIENT_ID=client_test_id
export OAUTH_CLIENT_SECRET=client_test_secret
export OAUTH_REDIRECT_URL=http://localhost:8001/auth/dexidp/callback/
export OAUTH_OIDC_CONFIG_ENDPOINT=http://127.0.0.1:5556/dex/.well-known/openid-configuration

export PGUSER=postgres
export PGPASSWORD=postgres
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=$POSTGRES_DB

dropdb $POSTGRES_DB
createdb $POSTGRES_DB

fractalctl set-db
fractalctl init-db-data --resource default --profile default --admin-email admin@example.org --admin-pwd 1234 --project-dir /fake
fractalctl start --port 8001
