#!/bin/bash

N=$1

TOKEN=$(
    curl -X POST \
        -d "username=admin@fractal.xy&password=1234" \
        http://localhost:8000/auth/token/login/ | jq -r ".access_token"
);

email=user${N}@fractal.xy
password=$(python3 -c "import secrets; print(secrets.token_hex(${N}))")

echo email ${email}
echo password ${password}

curl -X POST \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"${email}\",\"password\":\"${password}\",\"project_dirs\":[\"/tmp/fractal\"]}" \
  http://localhost:8000/auth/register/
