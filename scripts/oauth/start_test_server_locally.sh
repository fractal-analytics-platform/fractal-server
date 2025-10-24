#!/bin/bash

set -e

export FRACTAL_RUNNER_BACKEND=local
export JWT_SECRET_KEY=jwt_secret_key
export JWT_EXPIRE_SECONDS=1000
export FRACTAL_HELP_URL=https://example.org/info

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

export FRACTAL_EMAIL_SENDER=sender@example.org
export FRACTAL_EMAIL_SMTP_SERVER=localhost
export FRACTAL_EMAIL_SMTP_PORT=1025
export FRACTAL_EMAIL_INSTANCE_NAME=test
export FRACTAL_EMAIL_RECIPIENTS=recipient1@example.org,recipient2@example.org
export FRACTAL_EMAIL_USE_STARTTLS=false
export FRACTAL_EMAIL_USE_LOGIN=true
export FRACTAL_EMAIL_PASSWORD=fakepassword

dropdb --if-exists $POSTGRES_DB
createdb $POSTGRES_DB

fractalctl set-db
fractalctl init-db-data --resource default --profile default --admin-email admin@example.org --admin-pwd 1234 --admin-project-dir /fake
fractalctl start --port 8001
