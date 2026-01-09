export JWT_SECRET_KEY=secret
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export POSTGRES_DB=fractal_test

dropdb ${POSTGRES_DB}
createdb ${POSTGRES_DB}


uv run fractalctl set-db
uv run fractalctl init-db-data \
    --resource default \
    --profile default \
    --admin-email admin@example.org \
    --admin-pwd passadmin \
    --admin-project-dir /tmp/fractal_admin_project_dir

uv run fractalctl start &
FASTAPI_PID=$!

# --------------

kill -9 "$FASTAPI_PID"
