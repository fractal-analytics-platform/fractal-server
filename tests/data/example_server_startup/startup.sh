#!/bin/bash



echo -e "\
DEPLOYMENT_TYPE=testing
JWT_SECRET_KEY=secret
SQLITE_PATH=`pwd`/test.db
FRACTAL_TASKS_DIR=`pwd`/FRACTAL_TASKS_DIR
FRACTAL_LOGGING_LEVEL=20
FRACTAL_RUNNER_BACKEND=local
FRACTAL_RUNNER_WORKING_BASE_DIR=`pwd`/artifacts
FRACTAL_ADMIN_DEFAULT_EMAIL=admin@fractal.xy
FRACTAL_ADMIN_DEFAULT_PASSWORD=1234
JWT_EXPIRE_SECONDS=84600
OAUTH_GITHUB_CLIENT_ID=0a40c05145a17b8ad852
OAUTH_GITHUB_CLIENT_SECRET=4504bab4cd8afb2f2c01195bc502535aeb43e68e
" > .fractal_server.env

rm test.db
rm -r FRACTAL_TASKS_DIR

# Create an empty db
poetry run fractalctl set-db

# Start the server
poetry run fractalctl start --port 8000
