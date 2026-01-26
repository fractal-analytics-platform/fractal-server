#!/bin/bash

docker compose -f scripts/oauth/docker-compose.yaml up -d

export OAUTH_CLIENT_NAME=dexidp
export OAUTH_CLIENT_ID=client_test_id
export OAUTH_CLIENT_SECRET=client_test_secret
export OAUTH_REDIRECT_URL=http://localhost:8001/auth/dexidp/callback/
export OAUTH_OIDC_CONFIG_ENDPOINT=http://127.0.0.1:5556/dex/.well-known/openid-configuration
export FRACTAL_EMAIL_SENDER=sender@example.org
export FRACTAL_EMAIL_SMTP_SERVER=localhost
export FRACTAL_EMAIL_SMTP_PORT=1025
export FRACTAL_EMAIL_INSTANCE_NAME=test
export FRACTAL_EMAIL_RECIPIENTS=recipient1@example.org,recipient2@example.org
export FRACTAL_EMAIL_USE_STARTTLS=false
export FRACTAL_EMAIL_USE_LOGIN=true
export FRACTAL_EMAIL_PASSWORD=fakepassword
export FRACTAL_HELP_URL=https://example.org/info

uv run --frozen pytest -m "oauth" -vv -s --durations=0

docker compose -f scripts/oauth/docker-compose.yaml down
