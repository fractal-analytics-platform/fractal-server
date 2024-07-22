Terminal 1
```
docker run -p 5556:5556 ghcr.io/fractal-analytics-platform/oauth:0.1
```

Terminal 2
```
export FRACTAL_TASKS_DIR=/dev/fractal/task
export FRACTAL_RUNNER_WORKING_BASE_DIR=/dev/fractal/base_dir
export FRACTAL_RUNNER_BACKEND=local
export JWT_SECRET_KEY=jwt_secret_key
export JWT_EXPIRE_SECONDS=1000
export DB_ENGINE=sqlite
export SQLITE_PATH=fractal.sqlite
export OAUTH_DEXIDP_CLIENT_ID=client_test_id
export OAUTH_DEXIDP_CLIENT_SECRET=client_test_secret
export OAUTH_DEXIDP_REDIRECT_URL=http://localhost:8001/auth/dexidp/callback/
export OAUTH_DEXIDP_OIDC_CONFIGURATION_ENDPOINT=http://127.0.0.1:5556/dex/.well-known/openid-configuration
fractalctl set-db
fractalctl start --port 8001
```

Terminal 3
```
bash -x oauth.sh
```
