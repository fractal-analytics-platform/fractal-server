
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
export PORT=8001
export DEX_PORT=5556

export OAUTH_TEST_OIDC_CONFIGURATION_ENDPOINT=http://127.0.0.1:${DEX_PORT}/dex/.well-known/openid-configuration


cleanup() {
    kill $(jobs -p)
    docker compose down
    rm ${SQLITE_PATH}
}
trap cleanup EXIT

docker compose up -d

poetry run fractalctl set-db
poetry run fractalctl start -p $PORT &

sleep 5

# Get admin token
ADMIN_TOKEN=$(
    wget -qO- --header="Content-Type: application/x-www-form-urlencoded" \
    --post-data="username=admin@fractal.xy&password=1234" \
    http://127.0.0.1:${PORT}/auth/token/login/ \
    | jq -r ".access_token"
)

# Assert that there is only ONE user
USER_LIST=$(
    wget -qO- --header="Authorization: Bearer ${ADMIN_TOKEN}" \
    http://127.0.0.1:${PORT}/auth/users/
)
if [ $(echo $USER_LIST | jq '. | length') != "1" ]; then
    exit 1
fi

# Authenticate (and register) another user with OAuth
AUTHORIZATION_URL=$(wget -qO- \
    http://127.0.0.1:${PORT}/auth/test/authorize/ \
    | jq -r ".authorization_url"
)
wget -qO- $AUTHORIZATION_URL

# Assert that there are TWO users
USER_LIST=$(
    wget -qO- --header="Authorization: Bearer ${ADMIN_TOKEN}" \
    http://127.0.0.1:${PORT}/auth/users/
)
if [ $(echo $USER_LIST | jq '. | length') != "2" ]; then
    exit 2
fi
