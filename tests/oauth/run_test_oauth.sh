#!/bin/bash
export FRACTAL_TASKS_DIR=/tmp/fractal/task
export FRACTAL_RUNNER_WORKING_BASE_DIR=/tmp/fractal/wbd
export FRACTAL_RUNNER_BACKEND=local

export JWT_SECRET_KEY=xxx
export JWT_EXPIRE_SECONDS=600000

export DB_ENGINE=sqlite
export SQLITE_PATH=test_oauth.sql

export OAUTH_TEST_CLIENT_ID=client_test_id
export OAUTH_TEST_CLIENT_SECRET=client_test_secret
export OAUTH_TEST_REDIRECT_URL=http://localhost:8001/auth/test/callback/
export OAUTH_TEST_OIDC_CONFIGURATION_ENDPOINT=http://127.0.0.1:5556/dex/.well-known/openid-configuration


cleanup() {
    kill $(jobs -p)
    docker compose down
    rm ${SQLITE_PATH}
}
trap cleanup EXIT

docker compose up -d
sleep 2

poetry run fractalctl set-db
poetry run fractalctl start -p 8001 &
sleep 5

# ----

AUTHORIZATION_URL=$(curl \
    http://127.0.0.1:8001/auth/test/authorize/ \
    | jq -r ".authorization_url"
)
TOKEN=$(
    curl -L --silent --output /dev/null --cookie-jar - $AUTHORIZATION_URL \
    | grep "fastapiusersauth" | awk '{print $NF}'
)
WHOAMI=$(
    curl -H "Authorization: Bearer $TOKEN" \
    http://127.0.0.1:8001/auth/current-user/
)

# ----

assert_equal() {
  if [ "$1" != "$2" ]; then
    echo "Error: $1 != $2"
    exit 1
  fi
}

USER_ID=$(echo $WHOAMI | jq -r ".id")
EMAIL=$(echo $WHOAMI | jq -r ".email")
IS_ACTIVE=$(echo $WHOAMI | jq -r ".is_active")
IS_SUPERUSER=$(echo $WHOAMI | jq -r ".is_superuser")
IS_VERIFIED=$(echo $WHOAMI | jq -r ".is_verified")
SLURM_USER=$(echo $WHOAMI | jq -r ".slurm_user")
CACHE_DIR=$(echo $WHOAMI | jq -r ".cache_dir")
USER_NAME=$(echo $WHOAMI | jq -r ".user_name")
SLURM_ACCOUNTS=$(echo $WHOAMI | jq -r ".slurm_accounts")


assert_equal $USER_ID "2"
assert_equal $EMAIL "kilgore@kilgore.trout"
assert_equal $IS_ACTIVE "true"
assert_equal $IS_SUPERUSER "false"
assert_equal $IS_VERIFIED "false"
assert_equal $SLURM_USER "null"
assert_equal $CACHE_DIR "null"
assert_equal $USER_NAME "null"
assert_equal $SLURM_ACCOUNTS "[]"
